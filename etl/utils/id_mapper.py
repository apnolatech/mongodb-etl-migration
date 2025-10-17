# -*- coding: utf-8 -*-
"""
ID mapping between MongoDB (ObjectId strings) and PostgreSQL (integers auto-increment)
"""

from typing import Optional, Dict
from sqlalchemy import text
from etl.utils.logger import logger


class IDMapper:
    """Maps MongoDB IDs to PostgreSQL IDs using mongo_id"""
    
    def __init__(self, postgres_connection):
        """
        Args:
            postgres_connection: PostgreSQL connection
        """
        self.connection = postgres_connection
        self._cache: Dict[str, Dict[str, int]] = {}
    
    def get_postgres_id(self, table_name: str, mongo_id: str) -> Optional[int]:
        """
        Gets the PostgreSQL ID for a given mongo_id
        
        Args:
            table_name: PostgreSQL table name
            mongo_id: MongoDB ID (string)
        
        Returns:
            PostgreSQL ID (integer) or None if not found
        """
        if not mongo_id:
            return None
        
        # Check cache
        cache_key = f"{table_name}:{mongo_id}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            with self.connection.engine.connect() as conn:
                query = text(f'SELECT id FROM "{table_name}" WHERE mongo_id = :mongo_id LIMIT 1')
                result = conn.execute(query, {"mongo_id": str(mongo_id)})
                row = result.fetchone()
                
                if row:
                    pg_id = row[0]
                    self._cache[cache_key] = pg_id
                    return pg_id
                
                return None
                
        except Exception as e:
            logger.warning(f"Error finding ID for {table_name}.mongo_id={mongo_id}: {e}")
            return None
    
    def build_cache_for_table(self, table_name: str):
        """
        Builds complete cache of mongo_id -> id for a table
        
        Args:
            table_name: Table name
        """
        try:
            with self.connection.engine.connect() as conn:
                query = text(f'SELECT id, mongo_id FROM "{table_name}" WHERE mongo_id IS NOT NULL')
                result = conn.execute(query)
                
                count = 0
                for row in result:
                    pg_id, mongo_id = row[0], row[1]
                    cache_key = f"{table_name}:{mongo_id}"
                    self._cache[cache_key] = pg_id
                    count += 1
                
                # Special handling for role table: also cache by role name
                if table_name == 'role':
                    query = text('SELECT id, name FROM role WHERE name IS NOT NULL')
                    result = conn.execute(query)
                    
                    for row in result:
                        role_id, role_name = row[0], row[1]
                        if role_name:  # Skip NULL values
                            cache_key = f"role_name:{role_name}"
                            self._cache[cache_key] = role_id
                            count += 1
                
                logger.info(f"Cache built for {table_name}: {count} mappings")
                
        except Exception as e:
            logger.error(f"Error building cache for {table_name}: {e}")
    
    def clear_cache(self):
        """Clears the cache"""
        self._cache.clear()
    
    def get_role_id_by_name(self, role_name: str) -> Optional[int]:
        """
        Gets the PostgreSQL ID for a role by its name (not mongo_id)
        
        Args:
            role_name: Role name (e.g. 'USER_ROLE', 'AGENTE_ROLE')
        
        Returns:
            PostgreSQL ID (integer) or None if not found
        """
        if not role_name:
            return None
        
        # MongoDB uses 'USER_ROLE', but PostgreSQL role.name has 'USER'
        # Remove '_ROLE' suffix if present
        clean_name = role_name.replace('_ROLE', '')
        
        # Check cache with special key for role names
        cache_key = f"role_name:{clean_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        try:
            with self.connection.engine.connect() as conn:
                # Query by the 'name' column, not 'role'
                query = text('SELECT id FROM role WHERE name = :role_name LIMIT 1')
                result = conn.execute(query, {"role_name": clean_name})
                row = result.fetchone()
                
                if row:
                    role_id = row[0]
                    self._cache[cache_key] = role_id
                    return role_id
                
                return None
                
        except Exception as e:
            logger.warning(f"Error finding role ID for role name '{role_name}': {e}")
            return None
    
    def add_to_cache(self, table_name: str, mongo_id: str, postgres_id: int):
        """
        Adds a mapping to cache
        
        Args:
            table_name: Table name
            mongo_id: MongoDB ID
            postgres_id: PostgreSQL ID
        """
        if mongo_id and postgres_id:
            cache_key = f"{table_name}:{mongo_id}"
            self._cache[cache_key] = postgres_id
    
    def get_cache_stats(self) -> dict:
        """Returns cache statistics"""
        return {
            'total_entries': len(self._cache),
            'tables': len(set(key.split(':')[0] for key in self._cache.keys()))
        }

