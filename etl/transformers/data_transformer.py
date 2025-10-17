# -*- coding: utf-8 -*-
"""
Main data transformer
"""

from typing import Dict, Any, Optional
from datetime import datetime

from etl.transformers.base_transformer import BaseTransformer
from etl.transformers.field_mapper import FieldMapper
from etl.utils.logger import logger
from etl.utils.validators import validator
from etl.utils.id_mapper import IDMapper
from etl.config import settings

# Import encryption only if enabled
EncryptionMigrator = None
if settings.ENABLE_ENCRYPTION_MIGRATION:
    try:
        from etl.utils.encryption import EncryptionMigrator
    except ImportError:
        logger.warning("Encryption migrator not available (Crypto module not installed)")


class DataTransformer(BaseTransformer):
    """Main transformer that applies mappings and validations"""
    
    def __init__(self, cassandra_id_mapper=None, postgres_id_mapper=None):
        """
        Args:
            cassandra_id_mapper: ID mapper for Cassandra (optional)
            postgres_id_mapper: ID mapper for PostgreSQL (optional)
        """
        self.field_mapper = FieldMapper()
        self.id_mapper = cassandra_id_mapper
        self.pg_id_mapper = postgres_id_mapper
        
        # Initialize encryption migrator if enabled
        self.encryption_migrator = None
        if settings.ENABLE_ENCRYPTION_MIGRATION:
            try:
                self.encryption_migrator = EncryptionMigrator(
                    old_key=settings.OLD_ENCRYPTION_KEY,
                    old_iv=settings.OLD_ENCRYPTION_IV,
                    new_key=settings.NEW_ENCRYPTION_KEY,
                    new_iv=settings.NEW_ENCRYPTION_IV
                )
                logger.info("Encryption migrator initialized")
            except Exception as e:
                logger.error(f"Error initializing encryption migrator: {e}")
                logger.warning("Will continue without encryption migration")
    
    def transform(self, source_data: Dict[str, Any], entity_name: str, destination: str = None) -> Dict[str, Any]:
        """
        Transforms data from MongoDB to target format
        
        Args:
            source_data: Source data (MongoDB)
            entity_name: Entity name
            destination: 'postgres' or 'cassandra' (if None, determines automatically)
        
        Returns:
            Transformed data
        """
        
        if not source_data:
            return {}
        
        # Determine destination if not specified
        if destination is None:
            destination = self._determine_destination(entity_name)
        
        # Apply field mapping
        transformed = FieldMapper.map_fields(source_data, entity_name, destination)
        
        # Apply additional transformations
        transformed = self._apply_custom_transformations(transformed, entity_name, destination)
        
        # Check if transformation returned None (e.g., message should be skipped)
        if transformed is None:
            return None
        
        # If Cassandra, generate IDs using id_mapper
        if destination == 'cassandra' and self.id_mapper:
            transformed = self._add_cassandra_ids(transformed, source_data, entity_name)
        
        # Clean null values if necessary
        if destination == 'cassandra':
            transformed = self._remove_none_values(transformed)
        
        # Final cleanup: Ensure no ObjectId remains (convert to string or None)
        transformed = self._convert_remaining_objectids(transformed)
        
        return transformed
    
    def transform_batch(self, source_data_list: list, entity_name: str, destination: str = None) -> list:
        """
        Transforms a batch of records
        
        Args:
            source_data_list: List of source data
            entity_name: Entity name
            destination: Data destination
        
        Returns:
            List of transformed data (filtered - None values are removed)
        """
        transformed_list = []
        total = len(source_data_list)
        
        for idx, source_data in enumerate(source_data_list, 1):
            try:
                transformed = self.transform(source_data, entity_name, destination)
                # Filter out None values (messages that should be skipped)
                if transformed is not None:
                    transformed_list.append(transformed)
                    
                # Log progress every 1000 records
                if idx % 1000 == 0:
                    logger.info(f"  Transformed: {idx}/{total} records for {entity_name}")
                    
            except Exception as e:
                logger.error(f"Error transforming record {idx}/{total}: {e}")
                logger.debug(f"Record with error: {source_data}")
        
        return transformed_list
    
    def validate(self, data: Dict[str, Any], entity_name: str = None) -> bool:
        """
        Validates that transformed data is correct
        
        Args:
            data: Data to validate
            entity_name: Entity name (optional)
        
        Returns:
            True if data is valid
        """
        
        if not data:
            return False
        
        # Basic validations
        if not isinstance(data, dict):
            return False
        
        # Validate that it has at least one ID
        has_id = any(key in data for key in ['id', 'user_id', 'room_id', 'message_id'])
        
        if not has_id:
            logger.warning(f"Record without ID: {data}")
            return False
        
        return True
    
    def _determine_destination(self, entity_name: str) -> str:
        """
        Determines destination based on configuration
        
        Args:
            entity_name: Entity name
        
        Returns:
            'postgres' or 'cassandra'
        """
        
        mapping = settings.COLLECTION_MAPPINGS.get(entity_name, {})
        strategy = mapping.get('strategy', 'postgres_only')
        
        if strategy == 'postgres_only':
            return 'postgres'
        elif strategy == 'cassandra_only':
            return 'cassandra'
        else:  # dual
            return 'postgres'  # Default to postgres for dual strategy
    
    def _apply_custom_transformations(self, data: Dict[str, Any], entity_name: str, destination: str) -> Dict[str, Any]:
        """
        Applies custom transformations according to entity
        
        Args:
            data: Data to transform
            entity_name: Entity name
            destination: Data destination
        
        Returns:
            Transformed data
        """
        
        # Entity-specific transformations
        if entity_name == 'user':
            data = self._transform_user(data, destination)
        elif entity_name == 'message':
            data = self._transform_message(data, destination)
        elif entity_name == 'room':
            data = self._transform_room(data, destination)
        elif entity_name == 'municipality':
            data = self._transform_municipality(data, destination)
        elif entity_name == 'parroquia':
            data = self._transform_parroquia(data, destination)
        elif entity_name == 'live':
            data = self._transform_live(data, destination)
        elif entity_name == 'channel':
            data = self._transform_channel(data, destination)
        elif entity_name == 'docs':
            data = self._transform_docs(data, destination)
        
        return data
    
    def _transform_user(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        User-specific transformations
        
        Business rules from Go migration:
        - Normalize email to lowercase
        - Format phone to digits only
        - Format DNI according to Venezuelan standard
        - Normalize gender values
        """
        
        # Normalize email to lowercase
        if data.get('email'):
            data['email'] = data['email'].lower().strip()
        
        # Validate and clean phone
        if data.get('phone_number') or data.get('phone'):
            phone_field = 'phone_number' if destination == 'postgres' else 'phone'
            phone = data.get(phone_field)
            if phone:
                # Remove non-numeric characters
                data[phone_field] = ''.join(filter(str.isdigit, str(phone)))
        
        # Format DNI with V-00000000 format (with dash between letter and numbers)
        if data.get('dni') or data.get('cedula'):
            dni_field = 'dni' if destination == 'cassandra' else 'dni'
            dni = data.get(dni_field) or data.get('cedula')
            if dni:
                data[dni_field] = validator.sanitize_dni(str(dni))
        
        # Normalize gender (handler.go lines 180-189)
        if data.get('gender'):
            gender = data.get('gender')
            if gender == 'Masculino':
                data['gender'] = 'M'
            elif gender == 'Femenino':
                data['gender'] = 'F'
            elif gender not in ['M', 'F']:
                data['gender'] = ''
        
        # Handle deleted users (isActive=false)
        if destination == 'postgres':
            is_active = data.get('isActive', True)
            if not is_active:
                # Save original data in deleted_ fields
                data['deleted_name'] = data.get('name')
                data['deleted_phone'] = data.get('phone')
                data['deleted_email'] = data.get('email')
                data['deleted_at'] = datetime.now()
                
                # Clear sensitive fields
                data['name'] = 'Deleted User'
                data['email'] = ''
                data['phone'] = ''
                data['dni'] = ''
            
            # Remove isActive flag (not needed in target)
            data.pop('isActive', None)
        
        # Resolve foreign keys for PostgreSQL
        if destination == 'postgres' and self.pg_id_mapper:
            # Resolve province_id
            province_mongo_id = data.get('province')
            if province_mongo_id:
                if isinstance(province_mongo_id, dict):
                    province_mongo_id = str(province_mongo_id.get('_id', ''))
                province_id = self.pg_id_mapper.get_postgres_id('province', str(province_mongo_id))
                if province_id:
                    data['province_id'] = province_id
            
            # Resolve municipality_id
            municipality_mongo_id = data.get('municipality')
            if municipality_mongo_id:
                if isinstance(municipality_mongo_id, dict):
                    municipality_mongo_id = str(municipality_mongo_id.get('_id', ''))
                municipality_id = self.pg_id_mapper.get_postgres_id('municipality', str(municipality_mongo_id))
                if municipality_id:
                    data['municipality_id'] = municipality_id
            
            # Resolve parroquia_id
            parroquia_mongo_id = data.get('parroquia')
            if parroquia_mongo_id:
                if isinstance(parroquia_mongo_id, dict):
                    parroquia_mongo_id = str(parroquia_mongo_id.get('_id', ''))
                parroquia_id = self.pg_id_mapper.get_postgres_id('parroquia', str(parroquia_mongo_id))
                if parroquia_id:
                    data['parroquia_id'] = parroquia_id
            
            # Resolve roleId from userType field
            # userType contains role names like 'USER', 'ADMIN', 'DIRECTOR', etc.
            role_name = data.get('role')  # This now contains userType value from MongoDB
            if role_name:
                # role_name is a string like 'USER', 'ADMIN', etc., need to lookup by role name
                role_id = self.pg_id_mapper.get_role_id_by_name(str(role_name))
                if role_id:
                    data['roleId'] = role_id
                    logger.debug(f"Resolved roleId={role_id} for userType: {role_name}")
                else:
                    logger.warning(f"Could not resolve roleId for userType: {role_name}. Role might not exist in catalog.")
            else:
                logger.debug(f"User has no userType, roleId will be NULL")
            
            # Remove temporary fields used for lookup
            data.pop('province', None)
            data.pop('municipality', None)
            data.pop('parroquia', None)
            data.pop('role', None)
        
        return data
    
    def _transform_message(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        Message-specific transformations
        
        Business rules from Go migration:
        - Ensure content is not None
        - Validate UTF-8 encoding
        - Truncate very long content
        - Decrypt old encryption and re-encrypt with new system
        - Handle fileURL field (decrypt, replace bucket URL, handle mimeType)
        """
        
        # Ensure content is not None
        if data.get('content') is None:
            data['content'] = ''
        
        # ENCRYPTION MIGRATION
        # If content is encrypted with old system, migrate it
        content = data.get('content', '')
        
        if content and self.encryption_migrator:
            try:
                # Try to migrate encrypted content
                decrypted, new_encrypted, encryption_data = self.encryption_migrator.migrate_message(
                    encrypted_message=content,
                    log_decrypted=settings.LOG_DECRYPTED_MESSAGES
                )
                
                if decrypted is not None and new_encrypted is not None:
                    # Successful migration - message was encrypted with Salsa20
                    data['content'] = new_encrypted
                    
                    # encryption_data is only saved in room_details, NOT in messages_by_room
                    # For Cassandra, content is already encrypted and content_decrypted stores plain text
                    if destination == 'cassandra':
                        # In Cassandra we save both versions
                        data['content_decrypted'] = decrypted
                    else:
                        # In PostgreSQL we save encryption_data
                        data['encryption_data'] = encryption_data
                    
                    # Log for tracking
                    logger.debug(f"Encrypted message migrated from Salsa20 to AES-256-CBC")
                    
                    if settings.LOG_DECRYPTED_MESSAGES:
                        logger.debug(f"Original content: {decrypted[:200]}{'...' if len(decrypted) > 200 else ''}")
                        
                elif decrypted is not None and new_encrypted is None:
                    # Message was not encrypted, use as plain text
                    logger.debug(f"Message not encrypted, using plain text")
                    if destination == 'cassandra':
                        data['content_decrypted'] = decrypted
                    # Original content is kept
                else:
                    # Could not decrypt
                    logger.debug(f"Could not process message, keeping original")
                    
            except Exception as e:
                logger.warning(f"Error in encryption migration: {e}")
                logger.debug(f"Message content: {content[:100]}...")
                # Keep original content if migration fails
        
        # FILE URL HANDLING (from Go repo logic)
        # Decrypt fileURL if encrypted, apply replace logic, and handle mimeType
        file_url_encrypted = data.get('fileURL', '')
        file_type = data.get('fileType', '')
        
        if file_url_encrypted and file_url_encrypted != '':
            # Decrypt fileURL
            file_url_decrypted = ''
            if self.encryption_migrator:
                try:
                    # Decrypt using old encryption system
                    decrypted_url = self.encryption_migrator.old_encryption.decrypt(file_url_encrypted)
                    if decrypted_url:
                        file_url_decrypted = decrypted_url
                    else:
                        # Not encrypted, use as is
                        file_url_decrypted = file_url_encrypted
                except Exception as e:
                    logger.debug(f"Error decrypting fileURL: {e}")
                    file_url_decrypted = file_url_encrypted
            else:
                file_url_decrypted = file_url_encrypted
            
            # Apply replace logic (same as other files)
            from etl.transformers.field_mapper import FieldMapper
            file_url_replaced = FieldMapper._replace_gs_bucket(file_url_decrypted)
            
            # Handle mimeType based on fileType
            mime_type = ''
            skip_message = False
            
            if file_type == 'IMAGEN':
                mime_type = 'image/png'
            elif file_type == 'VIDEO':
                mime_type = 'video/mp4'
            elif file_type == 'AUDIO':
                mime_type = 'audio/mp3'
            elif file_type == 'DOCUMENTO':
                # Only allow PDF documents
                if file_url_replaced.endswith('.pdf'):
                    mime_type = 'application/pdf'
                else:
                    # Skip this message - unsupported document type
                    logger.debug(f"Skipping message with non-PDF document: {file_url_replaced}")
                    skip_message = True
            
            # If we need to skip the message, return None to signal it should be filtered out
            if skip_message:
                return None
            
            # Save file_url and mime_type (unencrypted)
            data['file_url'] = file_url_replaced
            data['mime_type'] = mime_type
        
        # Remove temporary fields used for transformation
        data.pop('fileURL', None)
        data.pop('fileType', None)
        
        # Validate UTF-8 encoding (migration_impl.go lines 1313-1325)
        content = data.get('content', '')
        try:
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            elif isinstance(content, str):
                content.encode('utf-8')
            data['content'] = content
        except (UnicodeDecodeError, UnicodeEncodeError):
            logger.warning("Invalid UTF-8 in message content, setting to empty")
            data['content'] = ''
        
        # Truncate very long content (for Cassandra)
        if destination == 'cassandra' and len(data.get('content', '')) > 10000:
            data['content'] = data['content'][:10000]
        
        # Validate timestamp
        if not data.get('created_at'):
            data['created_at'] = datetime.now()
        
        return data
    
    def _transform_room(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        Room-specific transformations
        
        Business rules from Go migration:
        - Ensure name is not empty
        - Validate room type
        - Set default permissions
        - Generate new encryptionData for the room
        - Mark for organizations table if isAdmin=true
        """
        
        # Ensure name is not empty
        if not data.get('name'):
            data['name'] = f"Room {data.get('room_id', 'unknown')}"
        
        # Mark room as organization if isAdmin=true
        # This will be handled separately in orchestrator
        data['_is_organization'] = data.get('isAdmin', False)
        
        # Remove isAdmin from data (not a column in room_details table)
        if 'isAdmin' in data:
            del data['isAdmin']
        
        # ROOM ENCRYPTION MIGRATION
        # Generate new encryptionData for room with new system using password "some password"
        if self.encryption_migrator:
            try:
                # Generate new encryptionData for room using password "some password"
                key_hex, iv_hex, encryption_data = self.encryption_migrator.new_encryption.generate_encryption_data(password="some password")
                data['encryption_data'] = encryption_data
                
                # Debug log (not INFO to avoid filling logs)
                logger.debug(f"New encryptionData generated for room (AES-256-CBC) using password")
                
            except Exception as e:
                logger.warning(f"Error generating encryptionData for room: {e}")
        
        # Validate room type (migration_impl.go lines 1145-1162)
        valid_types = ['p2p', 'group', 'broadcast']
        room_type = data.get('type')
        
        if not room_type:
            # Determine type from isGroup flag
            if data.get('is_group') or data.get('isGroup'):
                room_type = 'group'
            else:
                room_type = 'p2p'
            data['type'] = room_type
        
        elif room_type not in valid_types:
            data['type'] = 'p2p'
        
        # Set default permissions based on room type (migration_impl.go lines 1143-1162)
        if room_type == 'p2p':
            data['send_message'] = True
            data['add_member'] = True
            data['edit_group'] = True
        else:  # group
            can_write = data.get('canWrite', True)
            permissions = data.get('permissions', {})
            
            if isinstance(permissions, dict):
                data['send_message'] = permissions.get('canSendMessage', can_write)
            else:
                data['send_message'] = can_write
            
            data['add_member'] = False
            data['edit_group'] = False
        
        # member_count is not used in Cassandra room_details
        # Omitted to avoid unknown column error
        
        return data
    
    def _add_cassandra_ids(self, data: Dict[str, Any], source_data: Dict[str, Any], entity_name: str) -> Dict[str, Any]:
        """
        Adds generated IDs for Cassandra
        
        Args:
            data: Transformed data
            source_data: Original MongoDB data
            entity_name: Entity name
        
        Returns:
            Data with Cassandra IDs
        """
        mongo_id = str(source_data.get('_id', ''))
        
        if entity_name == 'user':
            data['user_id'] = self.id_mapper.get_user_id(mongo_id)
            # role_id from userType field - MongoDB stores userType as STRING (e.g. 'USER', 'ADMIN', 'DIRECTOR')
            role_name = source_data.get('userType')
            if role_name:
                # userType is a string like 'USER', 'ADMIN', etc.
                if isinstance(role_name, str):
                    role_id = self.id_mapper.get_role_id_by_name(role_name)
                    if role_id:
                        data['role_id'] = role_id
                        logger.debug(f"Resolved role_id={role_id} for userType: {role_name}")
                    else:
                        logger.warning(f"Could not resolve role_id for userType: {role_name}. Role might not exist in catalog.")
            # Remove temporary field used for lookup
            data.pop('role', None)
        
        elif entity_name == 'room':
            # Generate deterministic room_id based on mongo_id (UUID5)
            data['room_id'] = self.id_mapper.get_room_id(mongo_id)
        
        elif entity_name == 'message':
            # Generate room_id based on room mongo_id (use 'room' field, not 'roomId')
            room_mongo_id = source_data.get('room') or source_data.get('roomId')
            if room_mongo_id:
                data['room_id'] = self.id_mapper.get_room_id(str(room_mongo_id))
            data['message_id'] = self.id_mapper.get_message_id(mongo_id, data.get('created_at'))
            
            # Extract sender_id from 'sender' dict or fallback to 'userId'
            sender = source_data.get('sender') or source_data.get('userId')
            sender_mongo_id = ''
            
            if isinstance(sender, dict):
                sender_mongo_id = str(sender.get('_id', ''))
            elif sender:
                # sender might be an ObjectId string directly
                sender_mongo_id = str(sender)
            
            if sender_mongo_id and sender_mongo_id != 'None':
                # Get Cassandra user_id for this mongo_id
                # This uses CassandraIDMapper which generates sequential IDs
                cassandra_user_id = self.id_mapper.get_user_id(sender_mongo_id)
                data['sender_id'] = cassandra_user_id
                logger.debug(f"Message sender_id: {cassandra_user_id} (mongo_id: {sender_mongo_id})")
            else:
                logger.warning(f"Message {mongo_id} has no valid sender field")
            
            # reply_to_message_id if exists
            if source_data.get('replyTo'):
                data['reply_to_message_id'] = self.id_mapper.get_message_id(
                    str(source_data.get('replyTo')),
                    data.get('created_at')
                )
            
            # forwarded_from_message_id if exists
            if source_data.get('forwardedFrom'):
                data['forwarded_from_message_id'] = self.id_mapper.get_message_id(
                    str(source_data.get('forwardedFrom')),
                    data.get('created_at')
                )
            
            # Remove temporary fields used for lookup
            data.pop('room', None)
            data.pop('sender', None)
        
        elif entity_name == 'room_member':
            # Generate room_id and user_id using correct field names (room, user not roomId, userId)
            room_mongo_id = source_data.get('room') or source_data.get('roomId')
            user_mongo_id = source_data.get('user') or source_data.get('userId')
            if room_mongo_id:
                data['room_id'] = self.id_mapper.get_room_id(str(room_mongo_id))
            if user_mongo_id:
                data['user_id'] = self.id_mapper.get_user_id(str(user_mongo_id))
            # Remove temporary fields used for lookup
            data.pop('room', None)
            data.pop('user', None)
        
        elif entity_name == 'roles':
            data['id'] = self.id_mapper.get_role_id(mongo_id)
        
        return data
    
    def _remove_none_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Removes None values from dictionary (for Cassandra)
        
        Args:
            data: Dictionary with data
        
        Returns:
            Dictionary without None values
        """
        return {k: v for k, v in data.items() if v is not None}
    
    def _transform_municipality(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        Municipality-specific transformations
        Resolves province_id foreign key
        """
        if destination == 'postgres' and self.pg_id_mapper:
            # Resolve province_id from province mongo_id
            province_mongo_id = data.get('province') or data.get('province_id')
            if province_mongo_id:
                if isinstance(province_mongo_id, dict):
                    province_mongo_id = str(province_mongo_id.get('_id', ''))
                province_id = self.pg_id_mapper.get_postgres_id('province', str(province_mongo_id))
                if province_id:
                    data['province_id'] = province_id
            
            # Remove temporary field used for lookup
            data.pop('province', None)
        
        return data
    
    def _transform_parroquia(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        Parroquia-specific transformations
        Resolves province_id and municipality_id foreign keys
        """
        if destination == 'postgres' and self.pg_id_mapper:
            # Resolve province_id
            province_mongo_id = data.get('province') or data.get('province_id')
            if province_mongo_id:
                if isinstance(province_mongo_id, dict):
                    province_mongo_id = str(province_mongo_id.get('_id', ''))
                province_id = self.pg_id_mapper.get_postgres_id('province', str(province_mongo_id))
                if province_id:
                    data['province_id'] = province_id
            
            # Resolve municipality_id
            municipality_mongo_id = data.get('municipality') or data.get('municipality_id')
            if municipality_mongo_id:
                if isinstance(municipality_mongo_id, dict):
                    municipality_mongo_id = str(municipality_mongo_id.get('_id', ''))
                municipality_id = self.pg_id_mapper.get_postgres_id('municipality', str(municipality_mongo_id))
                if municipality_id:
                    data['municipality_id'] = municipality_id
            
            # Remove temporary fields used for lookup
            data.pop('province', None)
            data.pop('municipality', None)
        
        return data
    
    def _transform_live(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        Live-specific transformations
        Resolves channel_id and created_by foreign keys
        Handles deleted_at for inactive lives
        """
        if destination == 'postgres' and self.pg_id_mapper:
            # Resolve channel_id from channelId mongo_id
            channel_mongo_id = data.get('channelId')
            if channel_mongo_id and channel_mongo_id != '' and channel_mongo_id != 'None':
                # Clean the mongo_id (sometimes it might have extra spaces or formatting issues)
                channel_mongo_id = str(channel_mongo_id).strip()
                if channel_mongo_id:
                    channel_id = self.pg_id_mapper.get_postgres_id('channel', channel_mongo_id)
                    if channel_id:
                        data['channel_id'] = channel_id
                        logger.debug(f"Resolved channel_id {channel_id} for mongo_id: {channel_mongo_id}")
                    else:
                        logger.warning(f"Could not resolve channel_id for mongo_id: {channel_mongo_id}. Channel might not exist in database.")
            else:
                logger.debug(f"Live has no channel (channelId is empty or None)")
            
            # Resolve created_by from createdBy user mongo_id
            created_by_mongo_id = data.get('createdBy')
            if created_by_mongo_id:
                if isinstance(created_by_mongo_id, dict):
                    created_by_mongo_id = str(created_by_mongo_id.get('_id', ''))
                if created_by_mongo_id:
                    user_id = self.pg_id_mapper.get_postgres_id('user', str(created_by_mongo_id))
                    if user_id:
                        data['created_by'] = user_id
                    else:
                        logger.debug(f"Could not resolve created_by for user mongo_id: {created_by_mongo_id}")
            
            # Log if live is inactive and has deleted_at set
            if data.get('isActive') is False and data.get('deleted_at'):
                logger.debug(f"Live {data.get('mongo_id')} is inactive, deleted_at set to {data.get('deleted_at')}")
            
            # Remove temporary fields used for lookup
            data.pop('channelId', None)
            data.pop('createdBy', None)
            data.pop('isActive', None)  # Remove temporary field after processing
        
        return data
    
    def _transform_channel(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        Channel-specific transformations
        Resolves created_by foreign key
        """
        if destination == 'postgres' and self.pg_id_mapper:
            # Resolve created_by from createdBy user mongo_id
            created_by_mongo_id = data.get('createdBy')
            if created_by_mongo_id:
                if isinstance(created_by_mongo_id, dict):
                    created_by_mongo_id = str(created_by_mongo_id.get('_id', ''))
                user_id = self.pg_id_mapper.get_postgres_id('user', str(created_by_mongo_id))
                if user_id:
                    data['created_by'] = user_id
            
            # Remove temporary field used for lookup
            data.pop('createdBy', None)
        
        return data
    
    def _transform_docs(self, data: Dict[str, Any], destination: str) -> Dict[str, Any]:
        """
        Docs-specific transformations
        Resolves uploaded_by_id foreign key
        """
        if destination == 'postgres' and self.pg_id_mapper:
            # Resolve uploaded_by_id from uploadedBy user mongo_id
            uploaded_by_mongo_id = data.get('uploadedBy')
            if uploaded_by_mongo_id:
                if isinstance(uploaded_by_mongo_id, dict):
                    uploaded_by_mongo_id = str(uploaded_by_mongo_id.get('_id', ''))
                user_id = self.pg_id_mapper.get_postgres_id('user', str(uploaded_by_mongo_id))
                if user_id:
                    data['uploaded_by_id'] = user_id
            
            # Remove temporary field used for lookup
            data.pop('uploadedBy', None)
        
        return data
    
    def _convert_remaining_objectids(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Converts any remaining ObjectId instances to strings
        This is a safety measure to prevent ObjectId errors in PostgreSQL
        
        Args:
            data: Data dictionary
            
        Returns:
            Data with all ObjectIds converted to strings
        """
        from bson import ObjectId
        
        conversions_made = 0
        for key, value in list(data.items()):
            # Convert ObjectId to string
            if isinstance(value, ObjectId):
                data[key] = str(value)
                conversions_made += 1
                logger.debug(f"Converted ObjectId in field '{key}' to string")
            # Handle dict (nested ObjectId)
            elif isinstance(value, dict):
                if value == {} or not value:
                    data[key] = None
                elif '_id' in value:
                    if isinstance(value.get('_id'), ObjectId):
                        data[key] = str(value['_id'])
                        conversions_made += 1
                        logger.debug(f"Converted nested ObjectId in field '{key}' to string")
                    else:
                        # Dict but _id is not ObjectId, convert whole dict to None
                        data[key] = None
            # Handle empty containers
            elif isinstance(value, list):
                if value == [] or not value:
                    data[key] = None
        
        if conversions_made > 0:
            logger.debug(f"Converted {conversions_made} ObjectId fields to string")
        
        return data

