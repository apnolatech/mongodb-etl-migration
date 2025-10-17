# -*- coding: utf-8 -*-
"""
Data validators based on business rules from Go migration code
"""

import re
from typing import Dict, Any, Optional, List
from datetime import datetime


class DataValidator:
    """Validates data according to business rules"""
    
    # Valid user roles from Go code
    VALID_USER_ROLES = ['USER_ROLE', 'AGENTE_ROLE']
    
    # Invalid message types from Go code
    INVALID_MESSAGE_TYPES = ['GEO_LOCATION', 'STICKER', 'SURVEY', 'CONTACT']
    
    # Invalid message life types
    INVALID_MESSAGE_LIFE = ['TOUCH_SHOW_DELETE']
    
    # Valid room types
    VALID_ROOM_TYPES = ['p2p', 'group', 'broadcast']
    
    # Valid document types
    VALID_DOCUMENT_TYPES = ['FOLDER', 'FILE', 'IMAGEN', 'VIDEO', 'AUDIO', 'DOCUMENTO']
    
    @staticmethod
    def validate_user(user: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validates user data
        
        Note: All users are migrated regardless of role, phone, or active status
        to ensure complete data migration
        
        Args:
            user: User data dictionary
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        # Migrate ALL users to ensure complete migration
        return True, None
    
    @staticmethod
    def validate_room(room: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validates room data
        
        Note: All rooms are migrated (active, inactive, broadcast, diffusion)
        to ensure message integrity (messages reference all rooms)
        
        Args:
            room: Room data dictionary
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        # Migrate ALL rooms to ensure referential integrity with messages
        # No filtering by isActive, isBroadcast, or isDiffusion
        
        # Only validate room type if present
        room_type = room.get('type')
        if room_type and room_type not in DataValidator.VALID_ROOM_TYPES:
            # Even invalid types are migrated, just log a warning
            pass
        
        return True, None
    
    @staticmethod
    def validate_message(message: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validates message data based on Go migration rules
        
        Messages are skipped if:
        - MsgType is GEO_LOCATION, SURVEY, or CONTACT
        - MsgLife is TOUCH_SHOW_DELETE
        - IsActive is false
        
        Args:
            message: Message data dictionary
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        # Skip messages with invalid types
        msg_type = message.get('msgType') or message.get('type') or ''
        if msg_type in DataValidator.INVALID_MESSAGE_TYPES:
            return False, f"Message type {msg_type} is not supported"
        
        # Skip messages with invalid life types
        msg_life = message.get('msgLife') or message.get('life') or ''
        if msg_life in DataValidator.INVALID_MESSAGE_LIFE:
            return False, f"Message life {msg_life} is not supported"
        
        # Skip inactive messages
        is_active = message.get('isActive')
        if is_active is False:
            return False, "Message is not active"
        
        return True, None
    
    @staticmethod
    def validate_room_member(member: Dict[str, Any], room_type: str = None) -> tuple[bool, Optional[str]]:
        """
        Validates room member data
        
        Note: All room members are migrated to ensure complete chat membership
        
        Args:
            member: Member data dictionary
            room_type: Type of the room (p2p, group, etc.)
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        # Migrate ALL room members
        return True, None
    
    @staticmethod
    def validate_document(document: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validates document data
        
        Note: All documents are migrated regardless of type
        to ensure complete data migration
        
        Args:
            document: Document data dictionary
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        # Migrate ALL documents
        return True, None
    
    @staticmethod
    def validate_location_entity(entity: Dict[str, Any], entity_type: str) -> tuple[bool, Optional[str]]:
        """
        Validates geographic location entities (province, municipality, parroquia)
        
        Note: All location entities are migrated
        Foreign keys will be resolved in transformer if they exist
        
        Args:
            entity: Location entity data
            entity_type: Type of entity (province, municipality, parroquia)
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        # Ensure KeyInt has a value
        key_int = entity.get('keyInt') or entity.get('key_int')
        if not key_int or key_int == '':
            entity['keyInt'] = '0'
            entity['key_int'] = '0'
        
        # Migrate ALL location entities
        # Foreign keys are resolved in transformer
        return True, None
    
    @staticmethod
    def validate_phone_unique(phone: str, existing_phones: set) -> tuple[bool, Optional[str]]:
        """
        Validates phone uniqueness based on Go migration logic
        
        Business rules from migration_impl.go lines 734-815:
        - Check if phone already exists in database
        - Prevent duplicate insertions
        
        Args:
            phone: Phone number to validate
            existing_phones: Set of existing phone numbers
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        if phone in existing_phones:
            return False, f"Phone {phone} already exists"
        
        return True, None
    
    @staticmethod
    def sanitize_dni(dni: str) -> str:
        """
        Formats DNI according to Go utils.FormatDni
        
        Args:
            dni: DNI string
        
        Returns:
            Formatted DNI
        """
        if not dni:
            return dni
        
        # Remove non-alphanumeric characters
        dni = re.sub(r'[^A-Z0-9]', '', str(dni).upper())
        
        # Format as V-12345678 or E-12345678
        if len(dni) >= 7:
            if dni[0] in ['V', 'E', 'J', 'G', 'P']:
                return f"{dni[0]}-{dni[1:]}"
            else:
                return f"V-{dni}"
        
        return dni
    
    @staticmethod
    def validate_p2p_room_members(members: List[Dict[str, Any]]) -> tuple[bool, Optional[str]]:
        """
        Validates that p2p rooms have exactly 2 members
        
        Business rules from migration_impl.go lines 1188-1192
        
        Args:
            members: List of room members
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        if len(members) != 2:
            return False, f"P2P rooms must have exactly 2 members, found {len(members)}"
        
        return True, None
    
    @staticmethod
    def validate_batch(data_list: List[Dict[str, Any]], 
                      validator_func: callable, 
                      **kwargs) -> tuple[List[Dict[str, Any]], List[str]]:
        """
        Validates a batch of records
        
        Args:
            data_list: List of records to validate
            validator_func: Validation function to apply
            **kwargs: Additional arguments for validator
        
        Returns:
            Tuple of (valid_records, error_messages)
        """
        
        valid_records = []
        errors = []
        
        for idx, record in enumerate(data_list):
            is_valid, error_msg = validator_func(record, **kwargs)
            
            if is_valid:
                valid_records.append(record)
            else:
                errors.append(f"Record {idx}: {error_msg}")
        
        return valid_records, errors


# Singleton validator instance
validator = DataValidator()


