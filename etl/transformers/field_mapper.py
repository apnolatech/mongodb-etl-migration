# -*- coding: utf-8 -*-
"""
Field mapping between systems
"""

from typing import Dict, Any, Callable, Optional
from datetime import datetime
from uuid import uuid4


class FieldMapper:
    """Handles field mapping between MongoDB and destinations"""
    
    # ========================================================================
    # FIELD MAPPINGS FOR EACH ENTITY
    # ========================================================================
    
    FIELD_MAPPINGS = {
        # ====================================================================
        # USERS - Dual mapping (PostgreSQL + Cassandra)
        # ====================================================================
        'user': {
            'postgres': {
                'uuid': lambda doc: doc.get('uuid') or str(uuid4()),
                'avatar': lambda doc: FieldMapper._replace_gs_bucket(doc.get('photoURL') or doc.get('avatar')),
                'avatar_thumbnail': lambda doc: FieldMapper._replace_gs_bucket(doc.get('avatar_thumbnail')),
                'avatar_low_quality': lambda doc: FieldMapper._replace_gs_bucket(doc.get('avatar_low_quality')),
                # These fields will be handled specially in transformer for deleted users
                'phone': lambda doc: doc.get('phoneNumber') or doc.get('phone'),
                'phone_verified': lambda doc: doc.get('phoneVerified', False),
                'name': lambda doc: doc.get('displayName') or doc.get('name'),
                'fullName': lambda doc: doc.get('fullName') or doc.get('displayName') or doc.get('name'),
                # Note: password field is ignored (not migrated to new database)
                'email': lambda doc: doc.get('email'),
                'email_verified': lambda doc: doc.get('emailVerified', False),
                'description': lambda doc: doc.get('description') or doc.get('bio'),
                'ban_until': lambda doc: doc.get('ban_until'),
                'ban_at': lambda doc: doc.get('ban_at'),
                'removed_at': lambda doc: doc.get('removed_at'),
                'role_user': lambda doc: doc.get('role_user') or doc.get('role'),
                'profileStatus': lambda doc: 'COMPLETED',  # Always set to COMPLETED
                'deleted_by': lambda doc: doc.get('deleted_by'),
                'deleted_reason': lambda doc: doc.get('deleted_reason'),
                # Deleted user fields (will be filled in transformer if isActive=false)
                'deleted_at': lambda doc: None,  # Set in transformer
                'deleted_name': lambda doc: None,  # Set in transformer
                'deleted_phone': lambda doc: None,  # Set in transformer
                'deleted_email': lambda doc: None,  # Set in transformer
                # Extract ObjectIds from MongoDB for transformer resolution (convert to string to avoid type errors)
                'province': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('province')),  # Extract for transformer
                'municipality': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('municipality')),  # Extract for transformer
                'parroquia': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('parroquia')),  # Extract for transformer
                'role': lambda doc: doc.get('userType'),  # Extract userType from MongoDB for transformer
                'isActive': lambda doc: doc.get('isActive', True),  # Extract for transformer
                # Foreign key fields - will be resolved in transformer using ID mapper
                'province_id': lambda doc: None,  # Resolved in transformer
                'municipality_id': lambda doc: None,  # Resolved in transformer
                'parroquia_id': lambda doc: None,  # Resolved in transformer
                'roleId': lambda doc: None,  # Resolved in transformer
                'dni': lambda doc: doc.get('cedula') or doc.get('dni'),
                'birthdate': lambda doc: doc.get('birthDate') or doc.get('birthdate'),
                'gender': lambda doc: doc.get('gender'),
                'socialmedia_x': lambda doc: doc.get('socialMedia', {}).get('x') if isinstance(doc.get('socialMedia'), dict) else doc.get('socialmedia_x'),
                'socialmedia_instagram': lambda doc: doc.get('socialMedia', {}).get('instagram') if isinstance(doc.get('socialMedia'), dict) else doc.get('socialmedia_instagram'),
                'socialmedia_tiktok': lambda doc: doc.get('socialMedia', {}).get('tiktok') if isinstance(doc.get('socialMedia'), dict) else doc.get('socialmedia_tiktok'),
                'socialmedia_facebook': lambda doc: doc.get('socialMedia', {}).get('facebook') if isinstance(doc.get('socialMedia'), dict) else doc.get('socialmedia_facebook'),
                'socialmedia_whatsapp': lambda doc: doc.get('socialMedia', {}).get('whatsapp') if isinstance(doc.get('socialMedia'), dict) else doc.get('socialmedia_whatsapp'),
                'socialmedia_telegram': lambda doc: doc.get('socialMedia', {}).get('telegram') if isinstance(doc.get('socialMedia'), dict) else doc.get('socialmedia_telegram'),
                'ban_by': lambda doc: doc.get('ban_by'),
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            },
            'cassandra': {
                # Extract ObjectIds from MongoDB for transformer resolution
                'role': lambda doc: doc.get('userType'),  # Extract userType from MongoDB for transformer
                # user_id is generated in transformer using id_mapper
                'name': lambda doc: doc.get('displayName') or doc.get('name') or '',
                'email': lambda doc: doc.get('email') or '',
                'phone': lambda doc: doc.get('phoneNumber') or doc.get('phone') or '',
                'avatar': lambda doc: FieldMapper._replace_gs_bucket(doc.get('photoURL') or doc.get('avatar')) or '',
                # Note: dni column does not exist in Cassandra users table
                'is_bot': lambda doc: doc.get('isBot', False),
                # role_id is generated in transformer using id_mapper
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # ROOM - Cassandra only (room_details)
        # ====================================================================
        'room': {
            'cassandra': {
                # room_id is generated in transformer using id_mapper
                # Note: member_count does NOT exist in Cassandra room_details, omitted
                'name': lambda doc: doc.get('name', '') or f"Room {doc.get('_id', 'Unknown')}",
                'type': lambda doc: FieldMapper._get_room_type(doc),
                'description': lambda doc: doc.get('description') or '',
                'image': lambda doc: FieldMapper._replace_gs_bucket(doc.get('avatar') or doc.get('photo') or ''),
                'add_member': lambda doc: doc.get('permissions', {}).get('addMember', False) if isinstance(doc.get('permissions'), dict) else False,
                'edit_group': lambda doc: doc.get('permissions', {}).get('editGroup', False) if isinstance(doc.get('permissions'), dict) else False,
                'send_message': lambda doc: doc.get('canWrite', True) if 'canWrite' in doc else doc.get('permissions', {}).get('sendMessage', True),
                # Note: is_admin column does not exist in Cassandra room_details table
                # isAdmin is extracted for transformer to mark organizations, but not saved to room_details
                'isAdmin': lambda doc: doc.get('isAdmin', False),
                'join_all_user': lambda doc: doc.get('isPublic', False),
                'encryption_data': lambda doc: doc.get('encryptionData') or '',
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # MESSAGE - Cassandra only (messages_by_room)
        # ====================================================================
        'message': {
            'cassandra': {
                # Extract ObjectIds from MongoDB for transformer resolution
                'room': lambda doc: doc.get('room'),  # Extract for transformer
                'sender': lambda doc: doc.get('sender'),  # Extract for transformer
                # room_id, message_id, sender_id are generated in transformer using id_mapper
                'content': lambda doc: doc.get('message') or doc.get('content') or '',
                'content_decrypted': lambda doc: doc.get('decryptedMessage') or '',  # Filled in transformer if encrypted
                'type': lambda doc: doc.get('type') or 'user_message',  # Default to user_message
                'sender_message_id': lambda doc: str(doc.get('_id')) if doc.get('_id') else '',
                # reply_to_message_id, forwarded_from_message_id are generated in transformer
                # Note: file_url will be processed in transformer (decrypt, replace, extract from fileURL)
                'fileURL': lambda doc: doc.get('fileURL') or doc.get('fileUrl') or '',  # Extract for transformer
                'fileType': lambda doc: doc.get('fileType') or '',  # Extract for transformer
                'file_url': lambda doc: '',  # Filled in transformer after decryption
                'file_name': lambda doc: doc.get('fileName') or '',
                'file_size': lambda doc: doc.get('fileSize') or 0,
                'file_thumbnail_url': lambda doc: FieldMapper._replace_gs_bucket(doc.get('thumbnail') or ''),
                'mime_type': lambda doc: '',  # Filled in transformer based on fileType
                'audio_duration': lambda doc: doc.get('duration') or 0,
                'event': lambda doc: doc.get('event') or '',
                'is_deleted': lambda doc: doc.get('isDeleted', False),
                'deleted_at': lambda doc: doc.get('deletedAt') if doc.get('isDeleted') else None,
                'edited': lambda doc: doc.get('edited', False),
                # Note: mentions column does not exist in Cassandra messages_by_room table
                'poll_question': lambda doc: doc.get('pollQuestion') or '',
                'poll_is_multiple_options': lambda doc: doc.get('pollIsMultiple', False),
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # ROOM_MEMBER - Cassandra only (participants_by_room)
        # ====================================================================
        'room_member': {
            'cassandra': {
                # Extract ObjectIds from MongoDB for transformer resolution
                'room': lambda doc: doc.get('room'),  # Extract for transformer
                'user': lambda doc: doc.get('user'),  # Extract for transformer
                # room_id, user_id are generated in transformer using id_mapper
                'role': lambda doc: (doc.get('role', 'member') or 'member').upper(),  # Convert to uppercase
                'joined_at': lambda doc: doc.get('joinedAt', datetime.now()),
                'is_muted': lambda doc: doc.get('isMuted', False),
                'is_partner_blocked': lambda doc: doc.get('isPartnerBlocked', False),
                'mongo_id': lambda doc: str(doc.get('_id')),
            }
        },
        
        # ====================================================================
        # CHANNEL - PostgreSQL only
        # ====================================================================
        'channel': {
            'postgres': {
                'name': lambda doc: doc.get('name'),
                'description': lambda doc: doc.get('description'),
                'visible': lambda doc: doc.get('isActive', True) if isinstance(doc.get('isActive'), bool) else doc.get('visible', True),
                'picture': lambda doc: FieldMapper._replace_gs_bucket(doc.get('picture', {}).get('url') if isinstance(doc.get('picture'), dict) else doc.get('picture')),
                'cover': lambda doc: FieldMapper._replace_gs_bucket((doc.get('cover', {}).get('url') if isinstance(doc.get('cover'), dict) else doc.get('cover')) or (doc.get('coverImage', {}).get('url') if isinstance(doc.get('coverImage'), dict) else doc.get('coverImage'))),
                # Note: followers field is ignored
                'lives': lambda doc: len(doc.get('lives', [])) if isinstance(doc.get('lives'), list) else doc.get('lives', 0),
                'createdBy': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('createdBy')),  # Extract for transformer (convert ObjectId to string)
                'created_by': lambda doc: None,  # Resolved in transformer from user mongo_id
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # DOCS - PostgreSQL only
        # ====================================================================
        'docs': {
            'postgres': {
                'name': lambda doc: doc.get('title') or doc.get('name') or f"Document {str(doc.get('_id', 'unknown'))[:12]}",
                'description': lambda doc: doc.get('description') or '',
                'fileURL': lambda doc: FieldMapper._replace_gs_bucket((doc.get('fileUrl', {}).get('url') if isinstance(doc.get('fileUrl'), dict) else doc.get('fileUrl')) or (doc.get('fileURL', {}).get('url') if isinstance(doc.get('fileURL'), dict) else doc.get('fileURL')) or ''),
                'isVisible': lambda doc: doc.get('isVisible', True) if isinstance(doc.get('isVisible'), bool) else True,
                # isPrivate will be handled in transformer based on specialRole
                'isPrivate': lambda doc: doc.get('isPrivate', False),  # Overridden in transformer if specialRole exists
                # onFolder will be resolved in hierarchical processing (string path -> integer ID)
                'onFolder': lambda doc: 0,  # Default to 0 (root), resolved in _process_docs_hierarchical
                'type': lambda doc: doc.get('fileType') or doc.get('type') or 'FILE',  # Default to FILE if no type
                'uploadedBy': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('uploadedBy')),  # Extract for transformer (convert ObjectId to string)
                'uploaded_by_id': lambda doc: None,  # Resolved in transformer from user mongo_id
                # specialRole extraction for transformer
                'specialRole': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('specialRole')),  # Extract for transformer
                'size': lambda doc: doc.get('fileSize') or doc.get('size') or 0,
                'ext': lambda doc: doc.get('ext') or '',
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # LIVE - PostgreSQL only
        # ====================================================================
        'live': {
            'postgres': {
                'title': lambda doc: doc.get('title') or doc.get('name') or '',
                'description': lambda doc: doc.get('description') or '',
                # photo_url - try multiple field names and apply bucket replacement
                'photo_url': lambda doc: FieldMapper._extract_live_photo_url(doc),
                'status': lambda doc: 'FINISHED' if doc.get('isFinished', False) else 'LIVE',
                # live_url - try multiple field names
                'live_url': lambda doc: doc.get('liveUrl') or doc.get('url') or doc.get('videoUrl') or doc.get('link') or '',
                'live_type': lambda doc: 'YOUTUBE',  # Always YOUTUBE
                'viewers': lambda doc: 0,  # Default to 0 viewers
                'createdBy': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('createdBy')),  # Extract for transformer (convert ObjectId to string)
                'created_by': lambda doc: None,  # Resolved in transformer from user mongo_id
                'channelId': lambda doc: FieldMapper._extract_channel_id(doc.get('channel')),  # Extract mongo_id from channel
                'channel_id': lambda doc: None,  # Resolved in transformer from channelId mongo_id
                'category': lambda doc: 'FEATURED' if doc.get('isFeatured', False) else 'MORE_TO_SEE',
                # deleted_at - set to current time if isActive is false
                'deleted_at': lambda doc: datetime.now() if doc.get('isActive') is False else None,
                'isActive': lambda doc: doc.get('isActive'),  # Extract for transformer
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # PROVINCES - PostgreSQL only
        # ====================================================================
        'province': {
            'postgres': {
                'name': lambda doc: doc.get('name'),
                'key': lambda doc: doc.get('key') or doc.get('code'),
                'key_int': lambda doc: doc.get('key_int'),
                'is_active': lambda doc: doc.get('isActive', True),
                'latitude': lambda doc: doc.get('latitude'),
                'longitude': lambda doc: doc.get('longitude'),
                'key_dpt': lambda doc: doc.get('key_dpt'),
                'name_alt': lambda doc: doc.get('name_alt'),
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # MUNICIPALITIES - PostgreSQL only
        # ====================================================================
        'municipality': {
            'postgres': {
                'name': lambda doc: doc.get('name'),
                'key': lambda doc: doc.get('key') or doc.get('code'),
                'key_int': lambda doc: doc.get('key_int'),
                'is_active': lambda doc: doc.get('isActive', True),
                'latitude': lambda doc: doc.get('latitude'),
                'longitude': lambda doc: doc.get('longitude'),
                'key_dpt': lambda doc: doc.get('key_dpt'),
                'name_alt': lambda doc: doc.get('name_alt'),
                'province': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('province')),  # Extract province ObjectId for transformer
                'province_id': lambda doc: None,  # Resolved in transformer from province mongo_id
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # PARROQUIAS - PostgreSQL only
        # ====================================================================
        'parroquia': {
            'postgres': {
                'name': lambda doc: doc.get('name'),
                'key': lambda doc: doc.get('key') or doc.get('code'),
                'key_int': lambda doc: doc.get('key_int'),
                'is_active': lambda doc: doc.get('isActive', True),
                'latitude': lambda doc: doc.get('latitude'),
                'longitude': lambda doc: doc.get('longitude'),
                'key_dpt': lambda doc: doc.get('key_dpt'),
                'name_alt': lambda doc: doc.get('name_alt'),
                'province': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('province')),  # Extract province ObjectId for transformer
                'municipality': lambda doc: FieldMapper._safe_objectid_to_string(doc.get('municipality')),  # Extract municipality ObjectId for transformer
                'province_id': lambda doc: None,  # Resolved in transformer from province mongo_id
                'municipality_id': lambda doc: None,  # Resolved in transformer from municipality mongo_id
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # PROFESSIONS - PostgreSQL only
        # ====================================================================
        'profession': {
            'postgres': {
                'name': lambda doc: doc.get('name') or doc.get('name_es') or 'Unknown',
                'name_es': lambda doc: doc.get('name_es') or doc.get('name') or 'Desconocido',
                'is_active': lambda doc: doc.get('isActive', True),
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # ENTITIES - PostgreSQL only
        # ====================================================================
        'entity': {
            'postgres': {
                'name': lambda doc: doc.get('name'),
                'name_es': lambda doc: doc.get('name_es') or doc.get('name'),
                'is_active': lambda doc: doc.get('isActive', True),
                'mongo_id': lambda doc: str(doc.get('_id')),
                'organization': lambda doc: doc.get('organization'),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            }
        },
        
        # ====================================================================
        # ROLES - Dual (PostgreSQL + Cassandra)
        # ====================================================================
        'roles': {
            'postgres': {
                'name': lambda doc: doc.get('name'),
                'type': lambda doc: doc.get('type') or 'app',
                'default': lambda doc: doc.get('default', False),
                'mongo_id': lambda doc: str(doc.get('_id')),
                'created_at': lambda doc: doc.get('createdAt', datetime.now()),
                'updated_at': lambda doc: doc.get('updatedAt', datetime.now()),
            },
            'cassandra': {
                # id is generated in transformer using id_mapper
                'name': lambda doc: doc.get('name') or 'Unknown Role',
                'contacts_hide': lambda doc: doc.get('contactsHide', False),
                'mongo_id': lambda doc: str(doc.get('_id')),
            }
        }
    }
    
    # ========================================================================
    # HELPER METHODS
    # ========================================================================
    
    @staticmethod
    def _get_room_type(doc: Dict[str, Any]) -> str:
        """Determines room type"""
        if doc.get('isBroadcast'):
            return 'broadcast'
        elif doc.get('isGroup'):
            return 'group'
        else:
            return 'p2p'
    
    @staticmethod
    def _replace_gs_bucket(url: str) -> str:
        """
        Replaces Google Storage bucket URLs with new prefix
        
        Args:
            url: Original URL
            
        Returns:
            Updated URL with siscom_old/ prefix
        """
        if not url:
            return url
        
        # Replace old bucket URLs with new prefix (handle both with and without trailing slash)
        url = url.replace('gs://sibci-devel.appspot.com/', 'siscom_old/')
        url = url.replace('gs://sibci-2f7fc.appspot.com/', 'siscom_old/')
        url = url.replace('gs://sibci-devel.appspot.com', 'siscom_old/')
        url = url.replace('gs://sibci-2f7fc.appspot.com', 'siscom_old/')
        
        return url
    
    @staticmethod
    def _extract_live_photo_url(doc: Dict[str, Any]) -> str:
        """
        Extracts photo URL from live document (tries multiple field names)
        
        Args:
            doc: Live document
            
        Returns:
            Photo URL with bucket replacement applied
        """
        # Try multiple field names and formats
        photo_url = ''
        
        # Try photoURL (can be dict or string)
        if doc.get('photoURL'):
            photo_url_val = doc.get('photoURL')
            if isinstance(photo_url_val, dict):
                photo_url = photo_url_val.get('url', '')
            else:
                photo_url = photo_url_val
        
        # Try coverImage (can be dict or string)
        if not photo_url and doc.get('coverImage'):
            cover_val = doc.get('coverImage')
            if isinstance(cover_val, dict):
                photo_url = cover_val.get('url', '')
            else:
                photo_url = cover_val
        
        # Try thumbnail
        if not photo_url and doc.get('thumbnail'):
            thumbnail_val = doc.get('thumbnail')
            if isinstance(thumbnail_val, dict):
                photo_url = thumbnail_val.get('url', '')
            else:
                photo_url = thumbnail_val
        
        # Try cover
        if not photo_url and doc.get('cover'):
            cover_val = doc.get('cover')
            if isinstance(cover_val, dict):
                photo_url = cover_val.get('url', '')
            else:
                photo_url = cover_val
        
        # Try image
        if not photo_url and doc.get('image'):
            image_val = doc.get('image')
            if isinstance(image_val, dict):
                photo_url = image_val.get('url', '')
            else:
                photo_url = image_val
        
        # Apply bucket replacement
        return FieldMapper._replace_gs_bucket(photo_url) if photo_url else ''
    
    @staticmethod
    def _serialize_reactions(reactions: list) -> str:
        """Serializes reactions to JSON string"""
        import json
        return json.dumps(reactions) if reactions else '[]'
    
    @staticmethod
    def _serialize_list(items: list) -> str:
        """Serializes a list to JSON string"""
        import json
        return json.dumps(items) if items else '[]'
    
    @staticmethod
    def _safe_objectid_to_string(value: Any) -> Optional[str]:
        """
        Safely converts ObjectId or any value to string, handling edge cases
        
        Args:
            value: Value to convert (ObjectId, dict, string, etc.)
            
        Returns:
            String representation or None
        """
        if value is None:
            return None
        
        # Handle empty containers
        if isinstance(value, dict):
            if not value or value == {}:  # Empty dict
                return None
            # Extract _id from dict
            obj_id = value.get('_id')
            if obj_id:
                return str(obj_id)
            return None
        
        if isinstance(value, list):
            if not value or value == []:  # Empty list
                return None
            return None
        
        # Convert to string if has value
        value_str = str(value).strip()
        if value_str and value_str not in ['{}', '[]', 'None', '']:
            return value_str
        
        return None
    
    @staticmethod
    def _extract_channel_id(channel_value: Any) -> Optional[str]:
        """
        Extracts channel mongo_id from various formats
        
        Args:
            channel_value: Channel value (can be ObjectId, dict, string, list, etc.)
            
        Returns:
            Channel mongo_id as string or None
        """
        if channel_value is None:
            return None
        
        # Handle dict format: { "_id": ObjectId(...) }
        if isinstance(channel_value, dict):
            if not channel_value or channel_value == {}:
                return None
            obj_id = channel_value.get('_id')
            if obj_id:
                return str(obj_id)
            return None
        
        # Handle empty list
        if isinstance(channel_value, list):
            return None
        
        # Handle ObjectId or string
        value_str = str(channel_value).strip()
        if value_str and value_str not in ['{}', '[]', 'None', '']:
            return value_str
        
        return None
    
    # ========================================================================
    # PUBLIC METHODS
    # ========================================================================
    
    @classmethod
    def get_mapping(cls, entity_name: str, destination: str) -> Dict[str, Callable]:
        """
        Gets field mapping for an entity and destination
        
        Args:
            entity_name: Entity name
            destination: 'postgres' or 'cassandra'
        
        Returns:
            Dictionary with field mapping
        """
        entity_mappings = cls.FIELD_MAPPINGS.get(entity_name, {})
        return entity_mappings.get(destination, {})
    
    @classmethod
    def map_fields(cls, source_data: Dict[str, Any], entity_name: str, destination: str) -> Dict[str, Any]:
        """
        Maps fields from source format to destination format
        
        Args:
            source_data: Source data (MongoDB)
            entity_name: Entity name
            destination: 'postgres' or 'cassandra'
        
        Returns:
            Mapped data in destination format
        """
        mapping = cls.get_mapping(entity_name, destination)
        
        if not mapping:
            return {}
        
        result = {}
        
        for dest_field, mapper_func in mapping.items():
            try:
                value = mapper_func(source_data)
                result[dest_field] = value
            except Exception as e:
                # If mapping error, use None
                result[dest_field] = None
        
        return result

