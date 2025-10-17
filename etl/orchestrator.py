# -*- coding: utf-8 -*-
"""
Main ETL process orchestrator
"""

import time
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from etl.config import settings
from etl.utils import (
    DatabaseManager,
    logger,
    ETLMetrics,
    EntityMetrics,
    CassandraIDMapper,
    SchemaManager,
    IDMapper
)
from etl.extractors import MongoExtractor
from etl.transformers import DataTransformer
from etl.loaders import PostgresLoader, CassandraLoader


class ETLOrchestrator:
    """Main ETL process orchestrator"""
    
    def __init__(self, dry_run: bool = None, target_db: str = 'both', truncate_before: bool = False):
        """
        Args:
            dry_run: If True, does not load data (only extracts and transforms)
            target_db: Target database ('postgres', 'cassandra', 'both')
            truncate_before: If True, truncates all tables before migration
        """
        self.dry_run = dry_run if dry_run is not None else settings.DRY_RUN
        self.target_db = target_db.lower()
        self.truncate_before = truncate_before
        self.db_manager: Optional[DatabaseManager] = None
        self.metrics = ETLMetrics()
        
        # Validate target_db
        if self.target_db not in ['postgres', 'cassandra', 'both']:
            raise ValueError(f"target_db must be 'postgres', 'cassandra' or 'both', not '{target_db}'")
        
        # ETL components
        self.extractor: Optional[MongoExtractor] = None
        self.transformer: Optional[DataTransformer] = None
        self.postgres_loader: Optional[PostgresLoader] = None
        self.cassandra_loader: Optional[CassandraLoader] = None
        self.cassandra_id_mapper: Optional[CassandraIDMapper] = None
        self.postgres_id_mapper: Optional[IDMapper] = None
    
    def initialize(self):
        """Initializes connections and components"""
        logger.info("Initializing ETL...")
        logger.info(f"Target database: {self.target_db.upper()}")
        
        try:
            # Connect to databases
            self.db_manager = DatabaseManager()
            
            # Connect only to necessary databases
            self.db_manager.connect_mongo()
            
            if self.target_db in ['postgres', 'both']:
                self.db_manager.connect_postgres()
            
            if self.target_db in ['cassandra', 'both']:
                self.db_manager.connect_cassandra()
            
            # Verify and update schemas (add mongo_id columns if needed)
            SchemaManager.verify_and_update_schemas(self.db_manager, self.target_db)
            
            # Truncate tables before migration if requested
            if self.truncate_before and not self.dry_run:
                self._truncate_all_tables()
            
            # Initialize ETL components
            self.extractor = MongoExtractor(self.db_manager.mongo)
            
            # If target is PostgreSQL, initialize PostgreSQL ID mapper
            if self.target_db in ['postgres', 'both']:
                self.postgres_id_mapper = IDMapper(self.db_manager.postgres)
                logger.info("PostgreSQL IDMapper initialized")
                
                # Build caches for lookup tables
                # Note: Caches are built initially, then updated as new records are inserted
                logger.info("Building initial ID lookup caches...")
                try:
                    self.postgres_id_mapper.build_cache_for_table('province')
                    self.postgres_id_mapper.build_cache_for_table('municipality')
                    self.postgres_id_mapper.build_cache_for_table('parroquia')
                    self.postgres_id_mapper.build_cache_for_table('role')
                    self.postgres_id_mapper.build_cache_for_table('channel')
                    self.postgres_id_mapper.build_cache_for_table('profession')
                    self.postgres_id_mapper.build_cache_for_table('entities')
                    self.postgres_id_mapper.build_cache_for_table('user')
                    stats = self.postgres_id_mapper.get_cache_stats()
                    logger.info(f"ID cache built: {stats['total_entries']} mappings for {stats['tables']} tables")
                except Exception as e:
                    logger.warning(f"Could not build all caches (tables might be empty): {e}")
                    logger.info("Caches will be built as tables are populated")
            
            # If target is Cassandra, initialize Cassandra ID mapper
            if self.target_db in ['cassandra', 'both']:
                # Pass PostgreSQL connection to sync user IDs between databases
                postgres_conn = self.db_manager.postgres if self.target_db == 'both' else None
                self.cassandra_id_mapper = CassandraIDMapper(
                    cassandra_connection=self.db_manager.cassandra,
                    postgres_connection=postgres_conn
                )
                logger.info("CassandraIDMapper initialized")
                if postgres_conn:
                    logger.info("User ID sync enabled: Cassandra will use same IDs as PostgreSQL")
                
                # If truncate was executed, reset ID counters to start from 1
                if self.truncate_before:
                    logger.info("Resetting Cassandra ID counters after truncate...")
                    self.cassandra_id_mapper._next_user_id = 1
                    self.cassandra_id_mapper._next_role_id = 1
                    logger.info("  Cassandra ID counters will start from 1")
                
                # Show initial ID stats
                stats = self.cassandra_id_mapper.get_cache_stats()
                logger.info(f"Initial IDs - Next user_id: {stats['next_user_id']}, role_id: {stats['next_role_id']}")
            
            # Initialize transformer with ID mappers
            self.transformer = DataTransformer(
                cassandra_id_mapper=self.cassandra_id_mapper,
                postgres_id_mapper=self.postgres_id_mapper
            )
            
            if self.target_db in ['postgres', 'both']:
                self.postgres_loader = PostgresLoader(self.db_manager.postgres, self.postgres_id_mapper)
            
            if self.target_db in ['cassandra', 'both']:
                self.cassandra_loader = CassandraLoader(self.db_manager.cassandra)
            
            # Validate connections
            if not self._validate_connections():
                raise RuntimeError("Error in connection validation")
            
            logger.info("ETL initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing ETL: {e}")
            raise
    
    def run(self, entities: List[str] = None, parallel: bool = True):
        """
        Executes the complete ETL process
        
        Args:
            entities: List of entities to migrate (None = all)
            parallel: If True, processes entities in parallel
        """
        
        logger.info("=" * 80)
        logger.info("STARTING ETL PROCESS")
        logger.info("=" * 80)
        
        if self.dry_run:
            logger.warning("DRY RUN MODE - Data will not be loaded")
        
        if self.truncate_before:
            logger.warning("TRUNCATE MODE: ENABLED - Tables will be cleaned before migration")
        else:
            logger.info("TRUNCATE MODE: DISABLED - Assuming tables were cleaned with truncate_all_tables.py")
        
        try:
            # Initialize
            self.initialize()
            
            # Get list of entities to migrate
            entities_to_migrate = self._get_entities_to_migrate(entities)
            self.metrics.total_entities = len(entities_to_migrate)
            
            logger.info(f"Entities to migrate: {len(entities_to_migrate)}")
            
            # Process entities
            # Note: Sequential processing is used to maintain foreign key dependencies
            # Parent entities (province, municipality, role, channel) must be loaded before children
            if parallel and settings.MAX_WORKERS > 1 and len(entities_to_migrate) == 1:
                # Only use parallel mode for single entity (no dependencies)
                self._process_entities_parallel(entities_to_migrate)
            else:
                # Use sequential mode to respect entity order and dependencies
                self._process_entities_sequential(entities_to_migrate)
            
            # Post-migration phase: Populate complex denormalized tables
            if self.target_db in ['cassandra', 'both'] and not self.dry_run:
                self._post_migration_phase()
            
            # Finalize
            self.metrics.finish()
            self.metrics.print_summary()
            self.metrics.save_to_file()
            
            logger.info("=" * 80)
            logger.info("ETL PROCESS COMPLETED")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"Error in ETL process: {e}")
            raise
        
        finally:
            self.cleanup()
    
    def run_entity(self, entity_name: str, filters: dict = None) -> EntityMetrics:
        """
        Executes ETL for a single entity
        
        Args:
            entity_name: Entity name
            filters: Optional filters for extraction
        
        Returns:
            Entity metrics
        """
        
        logger.info("=" * 80)
        logger.info(f"PROCESSING ENTITY: {entity_name}")
        logger.info("=" * 80)
        
        entity_metrics = EntityMetrics(entity_name=entity_name)
        
        try:
            # Get entity configuration
            mapping = settings.COLLECTION_MAPPINGS.get(entity_name)
            if not mapping:
                logger.error(f"Configuration not found for: {entity_name}")
                return entity_metrics
            
            # Check if this is a many-to-many relationship table
            if mapping.get('is_many_to_many'):
                return self._process_many_to_many(entity_name, entity_metrics)
            
            strategy = mapping.get('strategy')
            logger.info(f"Strategy: {strategy}")
            
            # PHASE 1: EXTRACTION
            logger.info("Phase 1: Extraction from MongoDB...")
            extracted_data = list(self.extractor.extract(entity_name, filters))
            entity_metrics.records_extracted = len(extracted_data)
            logger.info(f"Extracted: {entity_metrics.records_extracted:,} records")
            
            if not extracted_data:
                logger.warning(f"No data to process for {entity_name}")
                entity_metrics.finish()
                return entity_metrics
            
            # PHASE 2: TRANSFORMATION AND LOADING
            # Check compatibility between strategy and target_db
            if not self._is_strategy_compatible(strategy):
                logger.warning(f"Entity {entity_name} with strategy '{strategy}' not compatible with target '{self.target_db}'. Skipping...")
                entity_metrics.finish()
                return entity_metrics
            
            if strategy == 'postgres_only':
                self._process_postgres_only(entity_name, extracted_data, entity_metrics)
            
            elif strategy == 'cassandra_only':
                self._process_cassandra_only(entity_name, extracted_data, entity_metrics)
            
            elif strategy == 'dual':
                self._process_dual(entity_name, extracted_data, entity_metrics)
            
            else:
                logger.error(f"Unknown strategy: {strategy}")
            
            # Finalize metrics
            entity_metrics.finish()
            
            # Reload role name mappings after roles are loaded to Cassandra
            if entity_name == 'roles' and strategy in ['cassandra_only', 'dual']:
                if self.cassandra_id_mapper and entity_metrics.records_loaded_cassandra > 0:
                    logger.info("Reloading role name mappings after roles insertion...")
                    self.cassandra_id_mapper._load_role_name_mappings()
            
            logger.info(f"Entity completed: {entity_name}")
            logger.info(f"  - Extracted: {entity_metrics.records_extracted:,}")
            logger.info(f"  - Transformed: {entity_metrics.records_transformed:,}")
            logger.info(f"  - Loaded to PostgreSQL: {entity_metrics.records_loaded_postgres:,}")
            logger.info(f"  - Loaded to Cassandra: {entity_metrics.records_loaded_cassandra:,}")
            logger.info(f"  - Errors: {entity_metrics.errors}")
            logger.info(f"  - Duration: {entity_metrics.duration_seconds:.2f}s")
            
        except Exception as e:
            logger.error(f"Error processing entity {entity_name}: {e}")
            entity_metrics.errors += 1
            entity_metrics.finish()
        
        return entity_metrics
    
    def _process_postgres_only(self, entity_name: str, extracted_data: list, metrics: EntityMetrics):
        """Processes entity for PostgreSQL only"""
        logger.info(f"Phase 2: Transformation for PostgreSQL ({len(extracted_data)} records)...")
        
        # Transform in batches
        batch_size = settings.BATCH_SIZE
        total_records = len(extracted_data)
        
        for i in range(0, len(extracted_data), batch_size):
            batch = extracted_data[i:i + batch_size]
            batch_end = min(i + len(batch), total_records)
            
            logger.info(f"  Transforming batch {i + 1}-{batch_end}/{total_records}...")
            
            # Transform
            transformed_batch = self.transformer.transform_batch(batch, entity_name, 'postgres')
            metrics.records_transformed += len(transformed_batch)
            
            # Load
            if not self.dry_run:
                logger.info(f"Phase 3: Loading batch {i + 1}-{batch_end}/{total_records} to PostgreSQL...")
                loaded = self.postgres_loader.load_batch(transformed_batch, entity_name)
                metrics.records_loaded_postgres += loaded
            else:
                metrics.records_loaded_postgres += len(transformed_batch)
    
    def _process_cassandra_only(self, entity_name: str, extracted_data: list, metrics: EntityMetrics):
        """Processes entity for Cassandra only"""
        total_records = len(extracted_data)
        logger.info(f"Phase 2: Transformation for Cassandra ({total_records} records)...")
        
        # Transform in batches
        batch_size = settings.BATCH_SIZE
        organizations_to_insert = []
        mongo_lookups_to_insert = []
        room_message_lookups_to_insert = []
        rooms_by_user_to_insert = []
        membership_lookups_to_insert = []
        
        for i in range(0, len(extracted_data), batch_size):
            batch = extracted_data[i:i + batch_size]
            batch_end = min(i + len(batch), total_records)
            
            logger.info(f"  Transforming batch {i + 1}-{batch_end}/{total_records}...")
            
            # Transform
            transformed_batch = self.transformer.transform_batch(batch, entity_name, 'cassandra')
            metrics.records_transformed += len(transformed_batch)
            
            # Check entity mappings
            mapping = settings.COLLECTION_MAPPINGS.get(entity_name, {})
            
            # Check if this entity has organizations (rooms with isAdmin=true)
            if mapping.get('has_organizations'):
                for record in transformed_batch:
                    if record.get('_is_organization'):
                        # Extract room_id and mongo_id for organizations table
                        organizations_to_insert.append({
                            'room_id': record.get('room_id'),
                            'mongo_id': record.get('mongo_id')
                        })
                    # Remove internal flag before saving to room_details
                    record.pop('_is_organization', None)
            
            # Check if this entity needs mongo_id lookup table (rooms_by_mongo)
            if mapping.get('has_mongo_lookup'):
                for record in transformed_batch:
                    mongo_id = record.get('mongo_id')
                    room_id = record.get('room_id')
                    if mongo_id and room_id:
                        mongo_lookups_to_insert.append({
                            'mongo_id': mongo_id,
                            'room_id': room_id
                        })
            
            # NOTE: p2p_room_by_users is NOT populated here anymore
            # The rooms collection does NOT contain a 'members' field
            # Instead, p2p_room_by_users is populated in the post-migration phase
            # after room_member migration to participants_by_room is complete
            
            # Check if this entity needs room_by_message lookup table
            if mapping.get('has_room_lookup'):
                for record in transformed_batch:
                    room_id = record.get('room_id')
                    message_id = record.get('message_id')
                    if room_id and message_id:
                        room_message_lookups_to_insert.append({
                            'message_id': message_id,
                            'room_id': room_id
                        })
            
            # Check if this entity needs rooms_by_user lookup table
            # NOTE: rooms_by_user is a complex denormalized table that requires:
            # - Clustering keys: is_pinned, last_message_at, room_id
            # - Room data, last message data, and membership data
            # It cannot be populated with just user_id and room_id
            # TODO: Implement proper rooms_by_user population in a separate phase
            # For now, we skip this to avoid clustering key errors
            if False and mapping.get('has_user_room_lookup'):  # Disabled
                for record in transformed_batch:
                    user_id = record.get('user_id')
                    room_id = record.get('room_id')
                    if user_id and room_id:
                        rooms_by_user_to_insert.append({
                            'user_id': user_id,
                            'room_id': room_id
                        })
            
            # Check if this entity needs room_membership_lookup table
            if mapping.get('has_membership_lookup'):
                for record in transformed_batch:
                    user_id = record.get('user_id')
                    room_id = record.get('room_id')
                    role = record.get('role', 'MEMBER')
                    if user_id and room_id:
                        membership_lookups_to_insert.append({
                            'user_id': user_id,
                            'room_id': room_id,
                            'role': role
                        })
            
            # Load
            if not self.dry_run:
                logger.info(f"Phase 3: Loading batch {i + 1}-{batch_end}/{total_records} to Cassandra...")
                loaded = self.cassandra_loader.load_batch(transformed_batch, entity_name)
                metrics.records_loaded_cassandra += loaded
            else:
                metrics.records_loaded_cassandra += len(transformed_batch)
        
        # Insert all lookup tables
        if not self.dry_run:
            self._insert_lookup_table('organizations', organizations_to_insert, 
                                      "INSERT INTO organizations (room_id, mongo_id) VALUES (?, ?)",
                                      lambda x: (x['room_id'], x['mongo_id']))
            
            self._insert_lookup_table('rooms_by_mongo', mongo_lookups_to_insert,
                                      "INSERT INTO rooms_by_mongo (mongo_id, room_id) VALUES (?, ?)",
                                      lambda x: (x['mongo_id'], x['room_id']))
            
            # NOTE: p2p_room_by_users is populated in post-migration phase, not here
            # (see _populate_p2p_room_by_users method)
            
            self._insert_lookup_table('room_by_message', room_message_lookups_to_insert,
                                      "INSERT INTO room_by_message (message_id, room_id) VALUES (?, ?)",
                                      lambda x: (x['message_id'], x['room_id']))
            
            # Disabled: rooms_by_user requires complex denormalized data (see note above)
            # self._insert_lookup_table('rooms_by_user', rooms_by_user_to_insert,
            #                           "INSERT INTO rooms_by_user (user_id, room_id) VALUES (?, ?)",
            #                           lambda x: (x['user_id'], x['room_id']))
            
            self._insert_lookup_table('room_membership_lookup', membership_lookups_to_insert,
                                      "INSERT INTO room_membership_lookup (user_id, room_id, role) VALUES (?, ?, ?)",
                                      lambda x: (x['user_id'], x['room_id'], x['role']))
    
    def _process_dual(self, entity_name: str, extracted_data: list, metrics: EntityMetrics):
        """Processes entity for PostgreSQL and/or Cassandra according to target_db"""
        batch_size = settings.BATCH_SIZE
        total_records = len(extracted_data)
        
        # Determine where to load according to target_db
        load_to_postgres = self.target_db in ['postgres', 'both']
        load_to_cassandra = self.target_db in ['cassandra', 'both']
        
        if load_to_postgres and load_to_cassandra:
            logger.info(f"Phase 2: Transformation for PostgreSQL and Cassandra ({total_records} records)...")
        elif load_to_postgres:
            logger.info(f"Phase 2: Transformation for PostgreSQL ({total_records} records)...")
        else:
            logger.info(f"Phase 2: Transformation for Cassandra ({total_records} records)...")
        
        for i in range(0, len(extracted_data), batch_size):
            batch = extracted_data[i:i + batch_size]
            batch_end = min(i + len(batch), total_records)
            
            logger.info(f"  Transforming batch {i + 1}-{batch_end}/{total_records}...")
            
            # Transform according to destination
            pg_batch = None
            cas_batch = None
            
            if load_to_postgres:
                pg_batch = self.transformer.transform_batch(batch, entity_name, 'postgres')
                metrics.records_transformed += len(pg_batch)
            
            if load_to_cassandra:
                cas_batch = self.transformer.transform_batch(batch, entity_name, 'cassandra')
                if not load_to_postgres:  # Only count once
                    metrics.records_transformed += len(cas_batch)
            
            # Load to configured destinations
            if not self.dry_run:
                logger.info(f"Phase 3: Loading batch {i + 1}-{batch_end}/{total_records}...")
                
                # PostgreSQL
                if load_to_postgres and pg_batch:
                    loaded_pg = self.postgres_loader.load_batch(pg_batch, entity_name)
                    metrics.records_loaded_postgres += loaded_pg
                
                # Cassandra
                if load_to_cassandra and cas_batch:
                    loaded_cas = self.cassandra_loader.load_batch(cas_batch, entity_name)
                    metrics.records_loaded_cassandra += loaded_cas
            else:
                if pg_batch:
                    metrics.records_loaded_postgres += len(pg_batch)
                if cas_batch:
                    metrics.records_loaded_cassandra += len(cas_batch)
    
    def _insert_lookup_table(self, table_name: str, records: list, insert_query: str, values_func):
        """
        Helper method to insert records into Cassandra lookup tables
        
        Args:
            table_name: Name of the lookup table
            records: List of records to insert
            insert_query: CQL INSERT query with placeholders
            values_func: Function to extract values tuple from record
        """
        if not records:
            return
        
        logger.info(f"Inserting {len(records)} records into {table_name}...")
        
        try:
            prepared = self.cassandra_loader.connection.session.prepare(insert_query)
            success_count = 0
            error_count = 0
            
            for record in records:
                try:
                    values = values_func(record)
                    self.cassandra_loader.connection.session.execute(prepared, values)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    # Log first few errors only
                    if error_count <= 5:
                        logger.error(f"  ERROR inserting into {table_name}: {e}")
            
            logger.info(f"  SUCCESS: Inserted {success_count}/{len(records)} records into {table_name}")
            if error_count > 5:
                logger.warning(f"  {error_count - 5} additional errors not logged")
                
        except Exception as e:
            logger.error(f"  ERROR: Failed to prepare statement for {table_name}: {e}")
    
    def _get_entity_filters(self, entity_name: str) -> dict:
        """
        Returns specific filters for each entity
        
        Args:
            entity_name: Entity name
        
        Returns:
            Dictionary with MongoDB filters
        """
        filters = {}
        
        # Filter users: only USER_ROLE and AGENTE_ROLE
        if entity_name == 'user':
            filters = {
                'role': {'$in': ['USER_ROLE', 'AGENTE_ROLE']}
            }
            logger.info(f"Applying filter for {entity_name}: role in ['USER_ROLE', 'AGENTE_ROLE']")
        
        return filters
    
    def _process_entities_sequential(self, entities: List[str]):
        """Processes entities sequentially"""
        logger.info("Sequential mode")
        
        for entity_name in entities:
            filters = self._get_entity_filters(entity_name)
            metrics = self.run_entity(entity_name, filters)
            self.metrics.add_entity_metrics(metrics)
    
    def _process_entities_parallel(self, entities: List[str]):
        """Processes entities in parallel"""
        logger.info(f"Parallel mode (workers: {settings.MAX_WORKERS})")
        
        with ThreadPoolExecutor(max_workers=settings.MAX_WORKERS) as executor:
            # Submit tasks with specific filters
            futures = {
                executor.submit(self.run_entity, entity_name, self._get_entity_filters(entity_name)): entity_name
                for entity_name in entities
            }
            
            # Collect results
            for future in as_completed(futures):
                entity_name = futures[future]
                try:
                    metrics = future.result()
                    self.metrics.add_entity_metrics(metrics)
                except Exception as e:
                    logger.error(f"Error processing {entity_name} in parallel: {e}")
    
    def _get_entities_to_migrate(self, entities: List[str] = None) -> List[str]:
        """
        Gets list of entities to migrate
        
        Args:
            entities: Specific list (None = all)
        
        Returns:
            List of entity names ordered by dependencies
        """
        
        if entities:
            return entities
        
        # Return all configured entities sorted by order field
        entity_list = []
        for entity_name, mapping in settings.COLLECTION_MAPPINGS.items():
            order = mapping.get('order', 999)
            entity_list.append((order, entity_name))
        
        # Sort by order and return entity names
        entity_list.sort(key=lambda x: x[0])
        return [name for order, name in entity_list]
    
    def _validate_connections(self) -> bool:
        """Validates that necessary connections are active"""
        logger.info("Validating connections...")
        
        valid = True
        
        # Always validate MongoDB (source)
        if not self.extractor.validate_connection():
            logger.error("MongoDB connection invalid")
            valid = False
        
        # Validate PostgreSQL only if necessary
        if self.target_db in ['postgres', 'both']:
            if self.postgres_loader and not self.postgres_loader.validate_connection():
                logger.error("PostgreSQL connection invalid")
                valid = False
        
        # Validate Cassandra only if necessary
        if self.target_db in ['cassandra', 'both']:
            if self.cassandra_loader and not self.cassandra_loader.validate_connection():
                logger.error("Cassandra connection invalid")
                valid = False
        
        if valid:
            logger.info("All required connections are valid")
        
        return valid
    
    def _is_strategy_compatible(self, strategy: str) -> bool:
        """
        Verifies if the strategy is compatible with the configured target_db
        
        Args:
            strategy: Entity strategy (postgres_only, cassandra_only, dual)
        
        Returns:
            True if compatible, False otherwise
        """
        if self.target_db == 'both':
            return True  # All strategies are compatible
        
        if self.target_db == 'postgres':
            return strategy in ['postgres_only', 'dual']
        
        if self.target_db == 'cassandra':
            return strategy in ['cassandra_only', 'dual']
        
        return False
    
    def _truncate_all_tables(self):
        """Truncates all tables before migration to ensure clean state"""
        logger.info("=" * 80)
        logger.info("TRUNCATING TABLES BEFORE MIGRATION")
        logger.info("=" * 80)
        
        # Truncate PostgreSQL tables
        if self.target_db in ['postgres', 'both'] and self.db_manager.postgres:
            self._truncate_postgresql()
        
        # Truncate Cassandra tables
        if self.target_db in ['cassandra', 'both'] and self.db_manager.cassandra:
            self._truncate_cassandra()
        
        logger.info("=" * 80)
    
    def _truncate_postgresql(self):
        """Truncates PostgreSQL tables in dependency order"""
        logger.info("Truncating PostgreSQL tables...")
        
        # Order: children → parents (reverse dependency order)
        tables = [
            'profession_user', 'entities_user',  # Many-to-many
            'live', 'docs',  # Depend on channel/user
            'user',  # Depends on role, locations
            'parroquia', 'municipality',  # Location hierarchy
            'channel', 'profession', 'entities', 'province', 'role'  # Independent
        ]
        
        for table in tables:
            try:
                with self.db_manager.postgres.get_session() as session:
                    from sqlalchemy import text
                    # RESTART IDENTITY reinicia las secuencias de auto-increment a 1
                    session.execute(text(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE'))
                logger.info(f"  SUCCESS: Truncated '{table}' (ID sequences restarted)")
            except Exception as e:
                if 'does not exist' not in str(e):
                    logger.warning(f"  Could not truncate '{table}': {e}")
    
    def _truncate_cassandra(self):
        """Truncates Cassandra tables in dependency order"""
        logger.info("Truncating Cassandra tables...")
        
        # Order: children → parents
        tables = [
            # Lookup tables (depend on messages, rooms, users)
            'room_by_message',
            'room_membership_lookup',
            'rooms_by_user',
            'p2p_room_by_users',
            # Messages and participants
            'messages_by_room',
            'participants_by_room',
            # Room lookup tables
            'organizations',
            'rooms_by_mongo',
            # Rooms
            'room_details',
            # Users
            'users',
            # Roles
            'role'
        ]
        
        for table in tables:
            try:
                self.db_manager.cassandra.session.execute(f"TRUNCATE {table}")
                logger.info(f"  SUCCESS: Truncated '{table}'")
            except Exception as e:
                if 'does not exist' not in str(e) and 'unconfigured table' not in str(e):
                    logger.warning(f"  Could not truncate '{table}': {e}")
    
    def _process_many_to_many(self, entity_name: str, metrics: EntityMetrics) -> EntityMetrics:
        """
        Processes many-to-many relationship tables
        Extracts arrays from source documents and creates relationship records
        
        Args:
            entity_name: Name of the relationship entity (profession_user, entities_user)
            metrics: Entity metrics
            
        Returns:
            Updated metrics
        """
        logger.info(f"Type: Many-to-many relationship")
        logger.info("Phase 1: Extraction from MongoDB users collection...")
        
        # Extract all users
        users = list(self.extractor.extract('user', {}))
        metrics.records_extracted = len(users)
        logger.info(f"Extracted: {len(users)} users")
        
        # Determine which array to process
        if entity_name == 'profession_user':
            array_field = 'profession'
            target_table = 'profession_user'
            foreign_table = 'profession'
        elif entity_name == 'entities_user':
            array_field = 'entities'
            target_table = 'entities_user'
            foreign_table = 'entities'
        else:
            logger.error(f"Unknown many-to-many entity: {entity_name}")
            return metrics
        
        logger.info(f"Phase 2: Extracting {array_field} relationships...")
        
        # Build relationship records
        relationship_records = []
        
        for user_doc in users:
            user_mongo_id = str(user_doc.get('_id', ''))
            if not user_mongo_id:
                continue
            
            # Get user_id from PostgreSQL
            user_id = self.postgres_id_mapper.get_postgres_id('user', user_mongo_id)
            if not user_id:
                continue
            
            # Get array of ObjectIds
            array_values = user_doc.get(array_field, [])
            if not isinstance(array_values, list):
                continue
            
            # Create relationship record for each item in array
            for item in array_values:
                if isinstance(item, dict):
                    item_mongo_id = str(item.get('_id', ''))
                else:
                    item_mongo_id = str(item)
                
                if not item_mongo_id:
                    continue
                
                # Get foreign key ID
                foreign_id = self.postgres_id_mapper.get_postgres_id(foreign_table, item_mongo_id)
                if not foreign_id:
                    logger.debug(f"Could not resolve {foreign_table}_id for mongo_id: {item_mongo_id}")
                    continue
                
                # Create relationship record
                if entity_name == 'profession_user':
                    relationship_records.append({
                        'user_id': user_id,
                        'profession_id': foreign_id,
                        'mongo_id': f"{user_mongo_id}_{item_mongo_id}"  # Composite for traceability
                    })
                else:  # entities_user
                    relationship_records.append({
                        'user_id': user_id,
                        'entities_id': foreign_id,  # Note: column name is 'entities_id' (plural)
                        'mongo_id': f"{user_mongo_id}_{item_mongo_id}"  # Composite for traceability
                    })
        
        metrics.records_transformed = len(relationship_records)
        logger.info(f"Created {len(relationship_records)} relationship records")
        
        # Load to PostgreSQL
        if relationship_records and not self.dry_run:
            logger.info(f"Phase 3: Loading to PostgreSQL...")
            loaded = self.postgres_loader.load_batch(relationship_records, entity_name)
            metrics.records_loaded_postgres = loaded
        else:
            metrics.records_loaded_postgres = len(relationship_records)
        
        metrics.finish()
        
        logger.info(f"Entity completed: {entity_name}")
        logger.info(f"  - Users processed: {metrics.records_extracted:,}")
        logger.info(f"  - Relationships created: {metrics.records_transformed:,}")
        logger.info(f"  - Loaded to PostgreSQL: {metrics.records_loaded_postgres:,}")
        logger.info(f"  - Duration: {metrics.duration_seconds:.2f}s")
        
        return metrics
    
    def _post_migration_phase(self):
        """
        Post-migration phase: Populates complex denormalized tables
        that require data from multiple already-migrated entities
        """
        logger.info("=" * 80)
        logger.info("POST-MIGRATION PHASE: Populating denormalized tables")
        logger.info("=" * 80)
        
        try:
            # Populate p2p_room_by_users (requires room_details and participants_by_room)
            self._populate_p2p_room_by_users()
            
            # Populate rooms_by_user (requires room_details, messages_by_room, and participants_by_room)
            self._populate_rooms_by_user()
            
        except Exception as e:
            logger.error(f"Error in post-migration phase: {e}")
            # Don't fail the entire migration, just log the error
            logger.warning("Post-migration phase had errors, but main migration completed")
    
    def _populate_p2p_room_by_users(self):
        """
        Populates p2p_room_by_users table from participants_by_room (OPTIMIZED)
        
        Table structure: (user1_id, user2_id, room_id)
        For bidirectional lookup, we insert both directions:
        - (userA, userB, room_id) - user A can find room with user B
        - (userB, userA, room_id) - user B can find room with user A
        
        Optimization: Uses concurrent execution for 10-50x faster performance
        """
        logger.info("\nPopulating p2p_room_by_users table (optimized)...")
        
        try:
            from cassandra.concurrent import execute_concurrent_with_args
            
            # Get all P2P rooms
            query = "SELECT room_id, type, name FROM room_details WHERE type = 'p2p' ALLOW FILTERING"
            p2p_rooms = list(self.cassandra_loader.connection.session.execute(query))
            total_p2p_rooms = len(p2p_rooms)
            
            if total_p2p_rooms == 0:
                logger.info("  No P2P rooms found, skipping p2p_room_by_users population")
                return
            
            logger.info(f"  Found {total_p2p_rooms:,} P2P rooms")
            
            # Prepare insert query with correct structure
            insert_query = "INSERT INTO p2p_room_by_users (user1_id, user2_id, room_id) VALUES (?, ?, ?)"
            prepared = self.cassandra_loader.connection.session.prepare(insert_query)
            
            # Collect all inserts to execute concurrently
            all_inserts = []
            rooms_without_participants = 0
            rooms_with_wrong_count = 0
            
            logger.info("  Building insert batch...")
            
            for idx, p2p_room in enumerate(p2p_rooms, 1):
                room_id = p2p_room.room_id
                
                try:
                    # Get participants for this room
                    participants_query = "SELECT user_id FROM participants_by_room WHERE room_id = %s"
                    participants = list(self.cassandra_loader.connection.session.execute(participants_query, [room_id]))
                    
                    # Skip if no participants
                    if not participants:
                        rooms_without_participants += 1
                        continue
                    
                    # Skip if less than 2 participants
                    if len(participants) != 2:
                        rooms_with_wrong_count += 1
                        if len(participants) < 2:
                            continue
                    
                    # Get first 2 user IDs
                    user_ids = [p.user_id for p in participants[:2]]
                    user1_id, user2_id = user_ids[0], user_ids[1]
                    
                    # Add both directions to batch
                    all_inserts.append((user1_id, user2_id, room_id))
                    all_inserts.append((user2_id, user1_id, room_id))
                    
                    # Progress log every 200 rooms
                    if idx % 200 == 0:
                        logger.info(f"  Progress: {idx:,}/{total_p2p_rooms:,} rooms scanned, {len(all_inserts):,} inserts prepared")
                        
                except Exception as e:
                    if idx <= 5:  # Only log first few errors
                        logger.error(f"  Error processing room {room_id}: {e}")
            
            if not all_inserts:
                logger.warning("  No valid P2P rooms to insert")
                return
            
            logger.info(f"  Executing {len(all_inserts):,} inserts concurrently (concurrency=100)...")
            
            # Execute all inserts concurrently (10-50x faster than sequential)
            # ULTRA-OPTIMIZED: concurrency=200 for 2M+ records
            results = execute_concurrent_with_args(
                self.cassandra_loader.connection.session,
                prepared,
                all_inserts,
                concurrency=200,
                raise_on_first_error=False
            )
            
            # Count successes and errors
            success_count = sum(1 for success, _ in results if success)
            error_count = sum(1 for success, _ in results if not success)
            
            logger.info(f"  ✓ p2p_room_by_users populated: {success_count:,}/{len(all_inserts):,} entries inserted (2 per room)")
            if error_count > 0:
                logger.warning(f"  ⚠ Errors: {error_count:,}")
            if rooms_without_participants > 0:
                logger.info(f"  ⚠ Rooms without participants: {rooms_without_participants:,}")
            if rooms_with_wrong_count > 0:
                logger.info(f"  ⚠ Rooms with non-standard participant count: {rooms_with_wrong_count:,}")
            
        except Exception as e:
            logger.error(f"Error populating p2p_room_by_users: {e}")
            raise
    
    def _populate_rooms_by_user(self):
        """
        Populates rooms_by_user table with denormalized room and message data (OPTIMIZED)
        This table is used for efficient user room listing with last message preview
        
        Optimization: Uses caching and batch processing for 10-100x faster performance
        """
        logger.info("\nPopulating rooms_by_user table (optimized)...")
        
        try:
            from cassandra.concurrent import execute_concurrent_with_args
            
            # Get all participants
            query = "SELECT user_id, room_id, role FROM participants_by_room"
            participants = list(self.cassandra_loader.connection.session.execute(query))
            total_participants = len(participants)
            
            if total_participants == 0:
                logger.info("  No participants found, skipping rooms_by_user population")
                return
            
            logger.info(f"  Found {total_participants:,} participants to process")
            
            # OPTIMIZATION 1: Pre-fetch all room details into cache
            logger.info("  Building room details cache...")
            room_cache = {}
            room_query = "SELECT * FROM room_details"
            for room in self.cassandra_loader.connection.session.execute(room_query):
                room_cache[room.room_id] = room
            logger.info(f"  Cached {len(room_cache):,} rooms")
            
            # OPTIMIZATION 2: Pre-fetch last messages per room
            logger.info("  Building last message cache...")
            last_message_cache = {}
            # Group participants by room to avoid duplicate queries
            rooms_with_participants = set(p.room_id for p in participants)
            for room_id in rooms_with_participants:
                msg_query = "SELECT * FROM messages_by_room WHERE room_id = %s ORDER BY message_id DESC LIMIT 1 ALLOW FILTERING"
                msg_rows = list(self.cassandra_loader.connection.session.execute(msg_query, [room_id]))
                if msg_rows:
                    last_message_cache[room_id] = msg_rows[0]
            logger.info(f"  Cached last messages for {len(last_message_cache):,} rooms")
            
            # OPTIMIZATION 3: Pre-fetch user info
            logger.info("  Building user info cache...")
            user_cache = {}
            user_query = "SELECT user_id, name, phone FROM users"
            for user in self.cassandra_loader.connection.session.execute(user_query):
                user_cache[user.user_id] = (user.name, user.phone)
            logger.info(f"  Cached {len(user_cache):,} users")
            
            # Prepare insert query
            insert_query = """
            INSERT INTO rooms_by_user (
                user_id, is_pinned, last_message_at, room_id,
                room_name, room_type, room_image, description,
                role, add_member, edit_group, send_message, join_all_user, encryption_data,
                is_muted,
                last_message_id, last_message_type, last_message_preview, last_message_sender_id,
                last_message_sender_name, last_message_sender_phone, last_message_file_url, 
                last_message_file_thumbnail_url, last_message_mime_type, last_message_status, 
                last_message_event, last_message_updated_at,
                created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?
            )
            """
            
            prepared = self.cassandra_loader.connection.session.prepare(insert_query)
            
            # OPTIMIZATION 4: Build all inserts first, then execute concurrently
            logger.info("  Building insert batch...")
            all_inserts = []
            skipped_count = 0
            
            for idx, participant in enumerate(participants, 1):
                user_id = participant.user_id
                room_id = participant.room_id
                role = participant.role or 'MEMBER'
                
                # Use cached room details
                room = room_cache.get(room_id)
                if not room:
                    skipped_count += 1
                    continue
                
                # Default values for last message
                last_message_id = None
                last_message_type = None
                last_message_preview = None
                last_message_sender_id = None
                last_message_sender_name = None
                last_message_sender_phone = None
                last_message_file_url = None
                last_message_file_thumbnail_url = None
                last_message_mime_type = None
                last_message_status = 0
                last_message_event = None
                last_message_at = room.created_at
                last_message_updated_at = room.updated_at
                
                # Use cached last message
                msg = last_message_cache.get(room_id)
                if msg:
                    last_message_id = msg.message_id
                    last_message_type = msg.type
                    last_message_preview = msg.content_decrypted[:100] if msg.content_decrypted else (msg.content[:100] if msg.content else '')
                    last_message_sender_id = msg.sender_id
                    last_message_file_url = msg.file_url
                    last_message_file_thumbnail_url = msg.file_thumbnail_url
                    last_message_mime_type = msg.mime_type
                    last_message_event = msg.event
                    last_message_at = msg.created_at
                    last_message_updated_at = msg.updated_at
                    
                    # Use cached sender info
                    if msg.sender_id and msg.sender_id in user_cache:
                        last_message_sender_name, last_message_sender_phone = user_cache[msg.sender_id]
                
                # Build insert values
                values = (
                    user_id, False, last_message_at, room_id,
                    room.name, room.type, room.image, room.description,
                    role, room.add_member, room.edit_group, room.send_message, room.join_all_user, room.encryption_data,
                    False,  # is_muted
                    last_message_id, last_message_type, last_message_preview, last_message_sender_id,
                    last_message_sender_name, last_message_sender_phone, last_message_file_url, 
                    last_message_file_thumbnail_url, last_message_mime_type, last_message_status, 
                    last_message_event, last_message_updated_at,
                    room.created_at, room.updated_at
                )
                
                all_inserts.append(values)
                
                # Progress log
                if idx % 1000 == 0:
                    logger.info(f"  Progress: {idx:,}/{total_participants:,} participants processed")
            
            if not all_inserts:
                logger.warning("  No valid rooms_by_user entries to insert")
                return
            
            logger.info(f"  Executing {len(all_inserts):,} inserts concurrently (concurrency=200)...")
            
            # OPTIMIZATION 5: Execute all inserts concurrently (50-100x faster)
            results = execute_concurrent_with_args(
                self.cassandra_loader.connection.session,
                prepared,
                all_inserts,
                concurrency=200,
                raise_on_first_error=False
            )
            
            # Count successes and errors
            success_count = sum(1 for success, _ in results if success)
            error_count = sum(1 for success, _ in results if not success)
            
            logger.info(f"  ✓ rooms_by_user populated: {success_count:,}/{len(all_inserts):,} records inserted")
            if error_count > 0:
                logger.warning(f"  ⚠ Errors: {error_count:,}")
            if skipped_count > 0:
                logger.warning(f"  ⚠ Skipped (room not found): {skipped_count:,}")
            
        except Exception as e:
            logger.error(f"Error populating rooms_by_user: {e}")
            raise
    
    def cleanup(self):
        """Cleans up resources and closes connections"""
        logger.info("Cleaning up resources...")
        
        if self.db_manager:
            self.db_manager.close_all()

