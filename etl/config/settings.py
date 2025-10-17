# -*- coding: utf-8 -*-
"""
ETL Configuration
"""

import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings:
    """Centralized configuration for ETL"""
    
    # ========================================================================
    # DIRECTORIES
    # ========================================================================
    BASE_DIR = Path(__file__).parent.parent.parent
    LOGS_DIR = BASE_DIR / 'etl' / 'logs'
    
    # ========================================================================
    # MONGODB (SOURCE)
    # ========================================================================
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
    MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'appcampana')
    MONGO_OPTIONS = {
        'maxPoolSize': 50,
        'minPoolSize': 10,
        'serverSelectionTimeoutMS': 5000,
        'connectTimeoutMS': 10000,
    }
    
    # ========================================================================
    # POSTGRESQL (TARGET)
    # ========================================================================
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', 'appcampana_db')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
    POSTGRES_OPTIONS = {
        'pool_size': 10,
        'max_overflow': 20,
        'pool_timeout': 30,
        'pool_recycle': 3600,
    }
    
    # ========================================================================
    # CASSANDRA (TARGET)
    # ========================================================================
    CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS', 'localhost').split(',')
    CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', '9042'))
    CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'appcampana')
    CASSANDRA_DATACENTER = os.getenv('CASSANDRA_DATACENTER', 'datacenter1')
    CASSANDRA_USER = os.getenv('CASSANDRA_USER', None)
    CASSANDRA_PASSWORD = os.getenv('CASSANDRA_PASSWORD', None)
    CASSANDRA_OPTIONS = {
        'protocol_version': 4,
        'load_balancing_policy': None,  # Will be configured at runtime
        'default_timeout': 60.0,  # Increased timeout for large batches
        'idle_heartbeat_interval': 30,  # Keep connection alive
        'connect_timeout': 10,
    }
    
    # Cassandra write consistency level for ETL
    # ONE = faster, works with single replica (suitable for migration)
    # QUORUM = requires majority of replicas (N/2 + 1)
    # ALL = requires all replicas
    CASSANDRA_WRITE_CONSISTENCY = os.getenv('CASSANDRA_WRITE_CONSISTENCY', 'ONE')
    
    # ========================================================================
    # ETL PARAMETERS
    # ========================================================================
    BATCH_SIZE = int(os.getenv('ETL_BATCH_SIZE', '10000'))  # Large batches for maximum performance
    MAX_WORKERS = int(os.getenv('ETL_MAX_WORKERS', '8'))  # More workers for parallel processing
    ENABLE_VALIDATION = os.getenv('ETL_ENABLE_VALIDATION', 'true').lower() == 'true'
    DRY_RUN = os.getenv('ETL_DRY_RUN', 'false').lower() == 'true'
    
    # ========================================================================
    # ENCRYPTION SETTINGS
    # ========================================================================
    # Old encryption (MongoDB - Salsa20)
    OLD_ENCRYPTION_KEY = os.getenv('OLD_ENCRYPTION_KEY', 'iL/Jq3ZGOuhhNDHvX0JNC54UAfJOoQcE')
    OLD_ENCRYPTION_IV = os.getenv('OLD_ENCRYPTION_IV', 'nlMMQoka')
    
    # New encryption (AES-256-CBC)
    NEW_ENCRYPTION_KEY = os.getenv('NEW_ENCRYPTION_KEY', '6f8ed8606d992a450e47d237d95db1139c2183cc305f04ac943cf35134481595')
    NEW_ENCRYPTION_IV = os.getenv('NEW_ENCRYPTION_IV', '0a4d522f5190d95a53f64da13f9d0c5c')
    
    # Enable/disable encryption migration
    ENABLE_ENCRYPTION_MIGRATION = os.getenv('ENABLE_ENCRYPTION_MIGRATION', 'true').lower() == 'true'
    LOG_DECRYPTED_MESSAGES = os.getenv('LOG_DECRYPTED_MESSAGES', 'false').lower() == 'true'
    
    # ========================================================================
    # MIGRATION STRATEGY
    # ========================================================================
    
    # Entities that go ONLY to PostgreSQL
    POSTGRES_ONLY = [
        'channel',
        'docs',
        'live',
        'provinces',
        'municipalities',
        'parroquias',
        'professions',
        'entities',
        'organizations',
        'centers',
        'ubch',
    ]
    
    # Entities that go ONLY to Cassandra
    CASSANDRA_ONLY = [
        'room',
        'message',
        'room_members',
        'rooms_by_user',
        'reactions_by_message',
        'message_seenby',
        'call',
        'call_member',
    ]
    
    # Entities that go to BOTH destinations
    DUAL_DESTINATION = [
        'user',
        'roles',
    ]
    
    # ========================================================================
    # COLLECTION MAPPINGS
    # ========================================================================
    # IMPORTANT: Order matters! Parent entities must be migrated before children
    # to ensure foreign key resolution works correctly
    COLLECTION_MAPPINGS = {
        # Step 1: Roles (needed for user.roleId)
        'roles': {
            'mongo': 'roles',
            'postgres': 'role',
            'cassandra': 'role',
            'strategy': 'dual',
            'order': 1
        },
        
        # Step 2: Geographic locations (needed for user foreign keys)
        'province': {
            'mongo': 'province',
            'postgres': 'province',
            'strategy': 'postgres_only',
            'order': 2
        },
        'municipality': {
            'mongo': 'municipality',
            'postgres': 'municipality',
            'strategy': 'postgres_only',
            'order': 3  # After province
        },
        'parroquia': {
            'mongo': 'parroquia',
            'postgres': 'parroquia',
            'strategy': 'postgres_only',
            'order': 4  # After municipality
        },
        
        # Step 3: Catalogs
        'profession': {
            'mongo': 'professions',
            'postgres': 'profession',
            'strategy': 'postgres_only',
            'order': 5
        },
        'entity': {
            'mongo': 'entities',
            'postgres': 'entities',
            'strategy': 'postgres_only',
            'order': 6
        },
        
        # Step 4: Users (after roles and locations)
        'user': {
            'mongo': 'users',
            'postgres': 'user',
            'cassandra': 'users',
            'strategy': 'dual',
            'order': 7
        },
        
        # Step 5: Content that depends on users
        'channel': {
            'mongo': 'channel',
            'postgres': 'channel',
            'strategy': 'postgres_only',
            'order': 8  # After users (for created_by)
        },
        'docs': {
            'mongo': 'docs',
            'postgres': 'docs',
            'strategy': 'postgres_only',
            'order': 9  # After users (for uploaded_by_id)
        },
        
        # Step 6: Lives (after channel and users)
        'live': {
            'mongo': 'live',
            'postgres': 'live',
            'strategy': 'postgres_only',
            'order': 10  # After channel and users
        },
        
        # Step 7: Chat (Cassandra only - after users)
        'room': {
            'mongo': 'rooms',
            'cassandra': 'room_details',
            'strategy': 'cassandra_only',
            'order': 11,
            'has_organizations': True,  # Rooms with isAdmin=true also go to organizations table
            'has_mongo_lookup': True,  # Also populates rooms_by_mongo lookup table
            'has_p2p_lookup': True  # Also populates p2p_room_by_users lookup table for P2P rooms
        },
        'message': {
            'mongo': 'messages',
            'cassandra': 'messages_by_room',
            'strategy': 'cassandra_only',
            'order': 12,  # After rooms
            'has_room_lookup': True  # Also populates room_by_message lookup table
        },
        'room_member': {
            'mongo': 'roommembers',
            'cassandra': 'participants_by_room',
            'strategy': 'cassandra_only',
            'order': 13,  # After rooms and users
            # NOTE: rooms_by_user is NOT populated from room_member anymore
            # It requires complex denormalized data (room details, last message, etc.)
            # TODO: Implement separate process to populate rooms_by_user
            'has_membership_lookup': True  # Populates room_membership_lookup table
        },
        
        # Step 8: Many-to-many relationships (PostgreSQL only - after users)
        # Note: These are extracted from user.profession and user.entities arrays
        'profession_user': {
            'mongo': 'users',  # Extracted from users.profession array
            'postgres': 'profession_user',
            'strategy': 'postgres_only',
            'order': 14,  # After users and professions
            'is_many_to_many': True
        },
        'entities_user': {
            'mongo': 'users',  # Extracted from users.entities array
            'postgres': 'entities_user',
            'strategy': 'postgres_only',
            'order': 15,  # After users and entities
            'is_many_to_many': True
        },
    }
    
    # ========================================================================
    # LOGGING
    # ========================================================================
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOG_FILE_MAX_BYTES = 10 * 1024 * 1024  # 10MB
    LOG_FILE_BACKUP_COUNT = 5
    
    @classmethod
    def get_postgres_url(cls) -> str:
        """Returns PostgreSQL connection URL"""
        return f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DATABASE}"
    
    @classmethod
    def validate(cls):
        """Validates that configuration is correct"""
        errors = []
        
        if not cls.MONGO_URI:
            errors.append("MONGO_URI is not configured")
        
        if not cls.POSTGRES_PASSWORD:
            errors.append("POSTGRES_PASSWORD is not configured")
        
        if cls.BATCH_SIZE < 1:
            errors.append("BATCH_SIZE must be greater than 0")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        return True


# Global configuration instance
settings = Settings()

