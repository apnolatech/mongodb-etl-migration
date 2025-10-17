# -*- coding: utf-8 -*-
"""
Database connection managers
"""

import logging
from typing import Optional
from contextlib import contextmanager

from pymongo import MongoClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

# Optional Cassandra import (not available in Python 3.12+)
try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
    from cassandra.query import BatchStatement, ConsistencyLevel
    CASSANDRA_AVAILABLE = True
except ImportError as e:
    CASSANDRA_AVAILABLE = False
    CASSANDRA_IMPORT_ERROR = str(e)
    logger = logging.getLogger(__name__)
    logger.warning(f"Cassandra driver not available: {e}")
    logger.warning("Cassandra functionality will be disabled. Use --target postgres to migrate only to PostgreSQL.")

from etl.config import settings

logger = logging.getLogger(__name__)


class MongoDBConnection:
    """MongoDB connection manager"""
    
    def __init__(self):
        self.client: Optional[MongoClient] = None
        self.db = None
    
    def connect(self):
        """Establishes connection to MongoDB"""
        try:
            logger.info(f"Connecting to MongoDB: {settings.MONGO_URI}")
            self.client = MongoClient(
                settings.MONGO_URI,
                **settings.MONGO_OPTIONS
            )
            self.db = self.client[settings.MONGO_DATABASE]
            
            # Verify connection
            self.client.server_info()
            logger.info("MongoDB connection established successfully")
            return self
            
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise
    
    def get_collection(self, collection_name: str):
        """Gets a MongoDB collection"""
        if self.db is None:
            raise RuntimeError("No MongoDB connection available")
        return self.db[collection_name]
    
    def close(self):
        """Closes the connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
    
    def __enter__(self):
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PostgreSQLConnection:
    """PostgreSQL connection manager"""
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
    
    def connect(self):
        """Establishes connection to PostgreSQL"""
        try:
            logger.info(f"Connecting to PostgreSQL: {settings.POSTGRES_HOST}")
            
            self.engine = create_engine(
                settings.get_postgres_url(),
                poolclass=QueuePool,
                pool_size=settings.POSTGRES_OPTIONS['pool_size'],
                max_overflow=settings.POSTGRES_OPTIONS['max_overflow'],
                pool_timeout=settings.POSTGRES_OPTIONS['pool_timeout'],
                pool_recycle=settings.POSTGRES_OPTIONS['pool_recycle'],
                echo=False
            )
            
            # Create sessionmaker
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            # Verify connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("PostgreSQL connection established successfully")
            return self
            
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Session:
        """Context manager to get a session"""
        if not self.SessionLocal:
            raise RuntimeError("No PostgreSQL connection available")
        
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def execute_raw(self, query: str, params: dict = None):
        """Executes a raw SQL query"""
        with self.engine.connect() as conn:
            return conn.execute(text(query), params or {})
    
    def close(self):
        """Closes the connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("PostgreSQL connection closed")
    
    def __enter__(self):
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class CassandraConnection:
    """Cassandra connection manager"""
    
    def __init__(self):
        if not CASSANDRA_AVAILABLE:
            raise ImportError(
                f"Cassandra driver is not available: {CASSANDRA_IMPORT_ERROR}\n"
                "Note: cassandra-driver is not compatible with Python 3.12+.\n"
                "To use only PostgreSQL, run with: --target postgres"
            )
        self.cluster: Optional[Cluster] = None
        self.session = None
    
    def connect(self):
        """Establishes connection to Cassandra"""
        if not CASSANDRA_AVAILABLE:
            raise ImportError("Cassandra driver is not available")
        
        try:
            logger.info(f"Connecting to Cassandra: {settings.CASSANDRA_HOSTS}")
            
            # Configure authentication if credentials are provided
            auth_provider = None
            if settings.CASSANDRA_USER and settings.CASSANDRA_PASSWORD:
                logger.info(f"Using authentication with user: {settings.CASSANDRA_USER}")
                auth_provider = PlainTextAuthProvider(
                    username=settings.CASSANDRA_USER,
                    password=settings.CASSANDRA_PASSWORD
                )
            
            # Configure load balancing policy
            load_balancing_policy = TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=settings.CASSANDRA_DATACENTER)
            )
            
            self.cluster = Cluster(
                contact_points=settings.CASSANDRA_HOSTS,
                port=settings.CASSANDRA_PORT,
                auth_provider=auth_provider,
                load_balancing_policy=load_balancing_policy,
                protocol_version=settings.CASSANDRA_OPTIONS['protocol_version'],
                connect_timeout=settings.CASSANDRA_OPTIONS.get('connect_timeout', 10),
                idle_heartbeat_interval=settings.CASSANDRA_OPTIONS.get('idle_heartbeat_interval', 30),
                control_connection_timeout=60.0,
                max_schema_agreement_wait=10,
            )
            
            self.session = self.cluster.connect()
            self.session.default_timeout = settings.CASSANDRA_OPTIONS['default_timeout']
            
            # Use keyspace
            self.session.set_keyspace(settings.CASSANDRA_KEYSPACE)
            
            logger.info("Cassandra connection established successfully")
            logger.info(f"Connection timeout: {settings.CASSANDRA_OPTIONS.get('connect_timeout')}s, Default query timeout: {settings.CASSANDRA_OPTIONS['default_timeout']}s")
            return self
            
        except Exception as e:
            logger.error(f"Error connecting to Cassandra: {e}")
            raise
    
    def execute(self, query: str, parameters: tuple = None):
        """Executes a CQL query"""
        if not self.session:
            raise RuntimeError("No Cassandra connection available")
        
        if parameters:
            return self.session.execute(query, parameters)
        return self.session.execute(query)
    
    def execute_batch(self, statements: list, consistency_level=None):
        """Executes a batch of statements"""
        if not self.session:
            raise RuntimeError("No Cassandra connection available")
        
        # Use provided consistency level or default from settings
        if consistency_level is None:
            consistency_map = {
                'ONE': ConsistencyLevel.ONE,
                'QUORUM': ConsistencyLevel.QUORUM,
                'ALL': ConsistencyLevel.ALL,
                'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
            }
            consistency_level = consistency_map.get(
                settings.CASSANDRA_WRITE_CONSISTENCY.upper(),
                ConsistencyLevel.ONE
            )
        
        batch = BatchStatement(consistency_level=consistency_level)
        
        for statement, params in statements:
            batch.add(statement, params)
        
        return self.session.execute(batch)
    
    def prepare(self, query: str):
        """Prepares a statement for multiple execution"""
        if not self.session:
            raise RuntimeError("No Cassandra connection available")
        return self.session.prepare(query)
    
    def close(self):
        """Closes the connection"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
    
    def __enter__(self):
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DatabaseManager:
    """Central manager for all connections"""
    
    def __init__(self):
        self.mongo: Optional[MongoDBConnection] = None
        self.postgres: Optional[PostgreSQLConnection] = None
        self.cassandra: Optional[CassandraConnection] = None
    
    def connect_mongo(self):
        """Connects to MongoDB only"""
        logger.info("Connecting to MongoDB...")
        self.mongo = MongoDBConnection().connect()
        return self
    
    def connect_postgres(self):
        """Connects to PostgreSQL only"""
        logger.info("Connecting to PostgreSQL...")
        self.postgres = PostgreSQLConnection().connect()
        return self
    
    def connect_cassandra(self):
        """Connects to Cassandra only"""
        if not CASSANDRA_AVAILABLE:
            raise ImportError(
                "Cassandra driver is not available. "
                "Use --target postgres to migrate only to PostgreSQL."
            )
        logger.info("Connecting to Cassandra...")
        self.cassandra = CassandraConnection().connect()
        return self
    
    def connect_all(self):
        """Connects to all databases"""
        logger.info("Initiating connections to all databases...")
        
        self.connect_mongo()
        self.connect_postgres()
        self.connect_cassandra()
        
        logger.info("All connections established")
        return self
    
    def close_all(self):
        """Closes all connections"""
        logger.info("Closing all connections...")
        
        if self.mongo:
            self.mongo.close()
        if self.postgres:
            self.postgres.close()
        if self.cassandra:
            self.cassandra.close()
        
        logger.info("All connections closed")
    
    def __enter__(self):
        return self.connect_all()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_all()

