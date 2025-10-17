# -*- coding: utf-8 -*-
"""
ID mapping between MongoDB (ObjectId strings) and Cassandra (UUIDs/integers)
Strategy: Full Migration without Lookup Tables
"""

from typing import Optional, Dict
import uuid
from datetime import datetime
from cassandra.util import uuid_from_time
from etl.utils.logger import logger


class CassandraIDMapper:
    """
    Generates IDs for Cassandra without using lookup tables.
    - Deterministic UUIDs for rooms (UUID5 - ensures consistency across references)
    - TimeUUIDs for messages (based on creation timestamp for temporal ordering)
    - Sequential INTs for users/roles
    """
    
    def __init__(self, cassandra_connection=None, postgres_connection=None):
        """
        Args:
            cassandra_connection: Cassandra connection (optional for offline mode)
            postgres_connection: PostgreSQL connection (optional, for user ID sync)
        """
        self.connection = cassandra_connection
        self.postgres_connection = postgres_connection
        self._room_cache: Dict[str, uuid.UUID] = {}
        self._user_cache: Dict[str, int] = {}
        self._message_cache: Dict[str, uuid.UUID] = {}
        self._role_cache: Dict[str, int] = {}
        self._role_name_cache: Dict[str, int] = {}  # Maps role name to role ID
        
        # Counters for generating sequential IDs
        self._next_user_id = 1
        self._next_role_id = 1
        
        # Load maximum existing IDs in Cassandra if connection available
        if self.connection:
            self._load_max_ids()
            self._load_role_name_mappings()
    
    def get_room_id(self, mongo_id: str, created_at: datetime = None) -> uuid.UUID:
        """
        Generates DETERMINISTIC UUID for room based on mongo_id
        CRITICAL: Always returns the same UUID for the same mongo_id
        This ensures messages reference the correct room
        
        Args:
            mongo_id: MongoDB ID (string)
            created_at: Not used - kept for API compatibility
        
        Returns:
            Cassandra UUID (deterministic, same for same mongo_id)
        """
        if not mongo_id:
            return uuid.uuid4()
        
        # Check cache - ALWAYS use cached value if exists
        if mongo_id in self._room_cache:
            return self._room_cache[mongo_id]
        
        # Generate DETERMINISTIC UUID from mongo_id using UUID5
        # This ensures the same mongo_id ALWAYS generates the same room_id
        # regardless of when or in what order it's called
        room_uuid = uuid.uuid5(uuid.NAMESPACE_OID, str(mongo_id))
        self._room_cache[mongo_id] = room_uuid
        
        return room_uuid
    
    def get_user_id(self, mongo_id: str) -> int:
        """
        Gets user ID, ensuring consistency between PostgreSQL and Cassandra
        
        If PostgreSQL connection is available, queries PostgreSQL for the user ID
        to ensure both databases use the same ID for the same user.
        
        Args:
            mongo_id: MongoDB ID (string)
        
        Returns:
            User ID (integer)
        """
        if not mongo_id:
            return self._generate_user_id()
        
        # Check cache
        if mongo_id in self._user_cache:
            return self._user_cache[mongo_id]
        
        # If PostgreSQL connection is available, query for existing user ID
        if self.postgres_connection:
            try:
                from sqlalchemy import text
                with self.postgres_connection.engine.connect() as conn:
                    query = text('SELECT id FROM "user" WHERE mongo_id = :mongo_id LIMIT 1')
                    result = conn.execute(query, {"mongo_id": str(mongo_id)})
                    row = result.fetchone()
                    
                    if row:
                        user_id = row[0]
                        self._user_cache[mongo_id] = user_id
                        logger.debug(f"User ID synced from PostgreSQL: {user_id} (mongo_id: {mongo_id})")
                        return user_id
            except Exception as e:
                logger.warning(f"Could not query PostgreSQL for user ID (mongo_id: {mongo_id}): {e}")
        
        # Generate new sequential ID if not found in PostgreSQL
        user_id = self._generate_user_id()
        self._user_cache[mongo_id] = user_id
        
        return user_id
    
    def get_message_id(self, mongo_id: str, created_at: datetime = None) -> uuid.UUID:
        """
        Generates TimeUUID for message based on timestamp
        
        Args:
            mongo_id: MongoDB ID (string)
            created_at: Message timestamp for generating TimeUUID
        
        Returns:
            Cassandra TimeUUID
        """
        if not mongo_id:
            return uuid_from_time(created_at or datetime.now())
        
        # Check cache
        if mongo_id in self._message_cache:
            return self._message_cache[mongo_id]
        
        # Generate TimeUUID from timestamp (preserves temporal order)
        message_uuid = uuid_from_time(created_at or datetime.now())
        self._message_cache[mongo_id] = message_uuid
        
        return message_uuid
    
    def get_role_id(self, mongo_id: str) -> int:
        """
        Generates sequential ID for role
        
        Args:
            mongo_id: MongoDB ID (string)
        
        Returns:
            Role ID (integer)
        """
        if not mongo_id:
            return self._generate_role_id()
        
        # Check cache
        if mongo_id in self._role_cache:
            return self._role_cache[mongo_id]
        
        # Generate new sequential ID
        role_id = self._generate_role_id()
        self._role_cache[mongo_id] = role_id
        
        return role_id
    
    def get_role_id_by_name(self, role_name: str) -> Optional[int]:
        """
        Gets role ID by role name (for Cassandra users)
        
        Args:
            role_name: Role name (e.g. 'USER_ROLE', 'AGENTE_ROLE', 'USER', 'AGENTE')
        
        Returns:
            Role ID (integer) or None if not found
        """
        if not role_name:
            return None
        
        # Try exact match first
        if role_name in self._role_name_cache:
            return self._role_name_cache[role_name]
        
        # Try without _ROLE suffix
        clean_name = role_name.replace('_ROLE', '')
        if clean_name in self._role_name_cache:
            return self._role_name_cache[clean_name]
        
        # Try with _ROLE suffix
        if not role_name.endswith('_ROLE'):
            with_suffix = f"{role_name}_ROLE"
            if with_suffix in self._role_name_cache:
                return self._role_name_cache[with_suffix]
        
        logger.debug(f"Could not find role ID for role name: {role_name}")
        return None
    
    def add_role_name_mapping(self, role_name: str, role_id: int, mongo_id: str = None):
        """
        Adds a role name mapping to cache (called after inserting a role)
        
        Args:
            role_name: Role name
            role_id: Role ID
            mongo_id: MongoDB ID (optional)
        """
        if role_name:
            self._role_name_cache[role_name] = role_id
            # Also cache with _ROLE suffix for compatibility
            if not role_name.endswith('_ROLE'):
                self._role_name_cache[f"{role_name}_ROLE"] = role_id
        
        if mongo_id:
            self._role_cache[mongo_id] = role_id
    
    def _load_max_ids(self):
        """Loads maximum existing IDs in Cassandra to continue sequence"""
        try:
            # Get max user_id
            result = self.connection.session.execute("SELECT MAX(user_id) as max_id FROM users")
            row = result.one()
            if row and row.max_id:
                self._next_user_id = row.max_id + 1
                logger.info(f"Max user_id in Cassandra: {row.max_id}, next: {self._next_user_id}")
            
            # Get max role_id
            result = self.connection.session.execute("SELECT MAX(id) as max_id FROM role")
            row = result.one()
            if row and row.max_id:
                self._next_role_id = row.max_id + 1
                logger.info(f"Max role_id in Cassandra: {row.max_id}, next: {self._next_role_id}")
                
        except Exception as e:
            logger.warning(f"Could not load maximum IDs from Cassandra: {e}")
    
    def _load_role_name_mappings(self):
        """Loads role name to role ID mappings from Cassandra"""
        try:
            result = self.connection.session.execute("SELECT id, name, mongo_id FROM role")
            for row in result:
                role_id = row.id
                role_name = row.name
                mongo_id = row.mongo_id
                
                # Cache by role name (e.g. "USER_ROLE" or "USER")
                if role_name:
                    self._role_name_cache[role_name] = role_id
                    # Also cache with _ROLE suffix for compatibility
                    if not role_name.endswith('_ROLE'):
                        self._role_name_cache[f"{role_name}_ROLE"] = role_id
                
                # Cache by mongo_id
                if mongo_id:
                    self._role_cache[mongo_id] = role_id
            
            logger.info(f"Loaded {len(self._role_name_cache)} role name mappings from Cassandra")
            
        except Exception as e:
            logger.warning(f"Could not load role name mappings from Cassandra: {e}")
    
    def _generate_user_id(self) -> int:
        """Generates a new sequential user_id"""
        user_id = self._next_user_id
        self._next_user_id += 1
        return user_id
    
    def _generate_role_id(self) -> int:
        """Generates a new sequential role_id"""
        role_id = self._next_role_id
        self._next_role_id += 1
        return role_id
    
    def clear_caches(self):
        """Clears all caches"""
        self._room_cache.clear()
        self._user_cache.clear()
        self._message_cache.clear()
        self._role_cache.clear()
        self._role_name_cache.clear()
    
    def get_cache_stats(self) -> dict:
        """Returns cache statistics"""
        return {
            'rooms': len(self._room_cache),
            'users': len(self._user_cache),
            'messages': len(self._message_cache),
            'roles': len(self._role_cache),
            'role_names': len(self._role_name_cache),
            'next_user_id': self._next_user_id,
            'next_role_id': self._next_role_id,
        }

