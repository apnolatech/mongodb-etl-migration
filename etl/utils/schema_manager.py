# -*- coding: utf-8 -*-
"""
Schema manager for automatic schema updates
Ensures all tables have required columns before migration
"""

from typing import Optional
from sqlalchemy import text
from etl.utils.logger import logger


class SchemaManager:
    """Manages schema updates for target databases"""
    
    @staticmethod
    def add_mongo_id_to_postgres(postgres_connection) -> bool:
        """
        Adds mongo_id column to all PostgreSQL tables if not exists
        
        Args:
            postgres_connection: PostgreSQL connection
            
        Returns:
            True if successful
        """
        postgres_tables = [
            'user', 'channel', 'docs', 'live',
            'province', 'municipality', 'parroquia',
            'profession', 'entities', 'role',
            'profession_user', 'entities_user'
        ]
        
        logger.info("Verifying mongo_id columns in PostgreSQL tables...")
        
        try:
            for table_name in postgres_tables:
                try:
                    with postgres_connection.get_session() as session:
                        # Check if table exists first
                        check_table_query = text(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = '{table_name}'
                            )
                        """)
                        result = session.execute(check_table_query)
                        table_exists = result.fetchone()[0]
                        
                        if not table_exists:
                            logger.debug(f"  - Table '{table_name}' does not exist, skipping")
                            continue
                        
                        # Add mongo_id column if not exists
                        alter_query = text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS mongo_id VARCHAR(255)')
                        session.execute(alter_query)
                        # Commit is automatic on context manager exit
                        logger.debug(f"  Ensured mongo_id exists in '{table_name}'")
                    
                except Exception as e:
                    logger.warning(f"  Could not update table '{table_name}': {e}")
            
            logger.info("PostgreSQL schema verification completed")
            return True
            
        except Exception as e:
            logger.error(f"Error updating PostgreSQL schema: {e}")
            return False
    
    @staticmethod
    def add_mongo_id_to_cassandra(cassandra_connection) -> bool:
        """
        Adds mongo_id column to all Cassandra tables if not exists
        
        Args:
            cassandra_connection: Cassandra connection
            
        Returns:
            True if successful
        """
        # Tables that need mongo_id column
        # Note: rooms_by_mongo is excluded because mongo_id is its PRIMARY KEY
        cassandra_tables = {
            'users': 'Users',
            'room_details': 'Room details',
            'messages_by_room': 'Messages by room',
            'participants_by_room': 'Participants by room',
            'role': 'Roles',
            'organizations': 'Organizations',
        }
        
        logger.info("Verifying mongo_id columns in Cassandra tables...")
        
        try:
            for table_name, description in cassandra_tables.items():
                try:
                    # Try to add the column
                    query = f"ALTER TABLE {table_name} ADD mongo_id text"
                    cassandra_connection.session.execute(query)
                    logger.debug(f"  Added mongo_id to '{table_name}'")
                    
                except Exception as e:
                    error_msg = str(e)
                    # Column already exists is not an error
                    if 'conflicts with an existing column' in error_msg or 'already exists' in error_msg.lower():
                        logger.debug(f"  mongo_id already exists in '{table_name}'")
                    else:
                        logger.warning(f"  Could not update table '{table_name}': {e}")
            
            logger.info("Cassandra schema verification completed")
            return True
            
        except Exception as e:
            logger.error(f"Error updating Cassandra schema: {e}")
            return False
    
    @staticmethod
    def add_missing_columns_to_cassandra(cassandra_connection) -> bool:
        """
        Adds missing columns to Cassandra tables if not exist
        
        Args:
            cassandra_connection: Cassandra connection
            
        Returns:
            True if successful
        """
        # Define missing columns for each table
        # Format: table_name: [(column_name, column_type, description)]
        missing_columns = {
            'messages_by_room': [
                ('file_name', 'text', 'File name'),
                ('file_size', 'bigint', 'File size in bytes'),
                ('audio_duration', 'int', 'Audio duration in seconds'),
            ],
            'room_membership_lookup': [
                ('role', 'text', 'User role in room'),
            ],
        }
        
        logger.info("Verifying missing columns in Cassandra tables...")
        
        try:
            for table_name, columns in missing_columns.items():
                for column_name, column_type, description in columns:
                    try:
                        # Try to add the column
                        query = f"ALTER TABLE {table_name} ADD {column_name} {column_type}"
                        cassandra_connection.session.execute(query)
                        logger.info(f"  Added {column_name} ({column_type}) to '{table_name}' - {description}")
                        
                    except Exception as e:
                        error_msg = str(e)
                        # Column already exists is not an error
                        if 'conflicts with an existing column' in error_msg or 'already exists' in error_msg.lower():
                            logger.debug(f"  - Column {column_name} already exists in '{table_name}'")
                        else:
                            logger.warning(f"  Could not add {column_name} to '{table_name}': {e}")
            
            logger.info("Cassandra missing columns verification completed")
            return True
            
        except Exception as e:
            logger.error(f"Error adding missing columns to Cassandra: {e}")
            return False
    
    @staticmethod
    def add_missing_columns_to_postgres(postgres_connection) -> bool:
        """
        Adds missing columns to PostgreSQL tables if not exist
        
        Args:
            postgres_connection: PostgreSQL connection
            
        Returns:
            True if successful
        """
        # Define missing columns for each table
        # Format: table_name: [(column_name, column_type, description)]
        missing_columns = {
            'live': [
                ('deleted_at', 'TIMESTAMP', 'Deletion timestamp for inactive lives'),
            ],
        }
        
        logger.info("Verifying missing columns in PostgreSQL tables...")
        
        try:
            for table_name, columns in missing_columns.items():
                try:
                    with postgres_connection.get_session() as session:
                        # Check if table exists first
                        check_table_query = text(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = '{table_name}'
                            )
                        """)
                        result = session.execute(check_table_query)
                        table_exists = result.fetchone()[0]
                        
                        if not table_exists:
                            logger.debug(f"  - Table '{table_name}' does not exist, skipping")
                            continue
                        
                        # Add each missing column
                        for column_name, column_type, description in columns:
                            try:
                                alter_query = text(f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{column_name}" {column_type}')
                                session.execute(alter_query)
                                logger.info(f"  Added {column_name} ({column_type}) to '{table_name}' - {description}")
                            except Exception as e:
                                logger.warning(f"  Could not add {column_name} to '{table_name}': {e}")
                    
                except Exception as e:
                    logger.warning(f"  Could not update table '{table_name}': {e}")
            
            logger.info("PostgreSQL missing columns verification completed")
            return True
            
        except Exception as e:
            logger.error(f"Error adding missing columns to PostgreSQL: {e}")
            return False
    
    @staticmethod
    def verify_and_update_schemas(db_manager, target_db: str = 'both') -> bool:
        """
        Verifies and updates schemas for all target databases
        
        Args:
            db_manager: DatabaseManager instance
            target_db: Target database ('postgres', 'cassandra', 'both')
            
        Returns:
            True if all updates successful
        """
        success = True
        
        logger.info("=" * 80)
        logger.info("SCHEMA VERIFICATION AND UPDATE")
        logger.info("=" * 80)
        
        # Update PostgreSQL schema
        if target_db in ['postgres', 'both'] and db_manager.postgres:
            logger.info("\n--- PostgreSQL Schema Updates ---")
            if not SchemaManager.add_mongo_id_to_postgres(db_manager.postgres):
                success = False
            if not SchemaManager.add_missing_columns_to_postgres(db_manager.postgres):
                success = False
        
        # Update Cassandra schema
        if target_db in ['cassandra', 'both'] and db_manager.cassandra:
            logger.info("\n--- Cassandra Schema Updates ---")
            if not SchemaManager.add_mongo_id_to_cassandra(db_manager.cassandra):
                success = False
            if not SchemaManager.add_missing_columns_to_cassandra(db_manager.cassandra):
                success = False
        
        logger.info("\n" + "=" * 80)
        if success:
            logger.info("All schema updates completed successfully")
        else:
            logger.warning("Some schema updates failed (check logs above)")
        
        logger.info("=" * 80)
        
        return success

