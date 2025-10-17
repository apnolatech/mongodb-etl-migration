# -*- coding: utf-8 -*-
"""
Cassandra data loader
"""

from typing import List, Dict, Any
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement

from etl.loaders.base_loader import BaseLoader
from etl.utils.database_connections import CassandraConnection
from etl.utils.logger import logger
from etl.config import settings


class CassandraLoader(BaseLoader):
    """Loads data to Cassandra"""
    
    def __init__(self, connection: CassandraConnection):
        """
        Args:
            connection: Cassandra connection
        """
        self.connection = connection
        self.prepared_statements = {}
        
        # Get consistency level from settings
        consistency_map = {
            'ONE': ConsistencyLevel.ONE,
            'TWO': ConsistencyLevel.TWO,
            'THREE': ConsistencyLevel.THREE,
            'QUORUM': ConsistencyLevel.QUORUM,
            'ALL': ConsistencyLevel.ALL,
            'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
            'LOCAL_ONE': ConsistencyLevel.LOCAL_ONE,
        }
        self.consistency_level = consistency_map.get(
            settings.CASSANDRA_WRITE_CONSISTENCY.upper(),
            ConsistencyLevel.ONE
        )
        logger.info(f"Using Cassandra write consistency level: {settings.CASSANDRA_WRITE_CONSISTENCY}")
    
    def load(self, data: Dict[str, Any], entity_name: str) -> bool:
        """
        Loads a record to Cassandra
        
        Args:
            data: Data to load
            entity_name: Entity name
        
        Returns:
            True if loading was successful
        """
        
        try:
            table_name = self._get_table_name(entity_name)
            
            if not table_name:
                logger.error(f"Table not found for entity: {entity_name}")
                return False
            
            # Build insertion query
            columns = list(data.keys())
            placeholders = ', '.join(['?'] * len(columns))
            columns_str = ', '.join(columns)
            
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # Prepare and execute
            prepared = self._get_prepared_statement(entity_name, query)
            values = tuple(data[col] for col in columns)
            
            self.connection.session.execute(prepared, values)
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading record to Cassandra ({entity_name}): {e}")
            logger.debug(f"Data: {data}")
            return False
    
    def load_batch(self, data_list: List[Dict[str, Any]], entity_name: str) -> int:
        """
        Loads a batch of records to Cassandra
        
        Args:
            data_list: List of data to load
            entity_name: Entity name
        
        Returns:
            Number of successfully loaded records
        """
        
        if not data_list:
            return 0
        
        table_name = self._get_table_name(entity_name)
        
        if not table_name:
            logger.error(f"Table not found for entity: {entity_name}")
            return 0
        
        loaded_count = 0
        
        try:
            # Prepare statement
            columns = list(data_list[0].keys())
            placeholders = ', '.join(['?'] * len(columns))
            columns_str = ', '.join(columns)
            
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            prepared = self._get_prepared_statement(entity_name, query)
            
            # Execute in batches
            batch = BatchStatement(consistency_level=self.consistency_level)
            batch_count = 0
            errors_in_batch = 0
            total_records = len(data_list)
            
            # Dynamic batch size based on entity (messages have large content)
            # Messages have large content (encrypted text, files) - use smaller batches
            # to avoid "Batch too large" errors
            batch_size = 100 if entity_name == 'message' else 500
            failed_records = []  # Track failed records for individual retry
            current_batch_records = []  # Track records in current batch
            
            for idx, data in enumerate(data_list, 1):
                try:
                    values = tuple(data.get(col) for col in columns)
                    batch.add(prepared, values)
                    batch_count += 1
                    current_batch_records.append(data)
                    
                    # Execute batch when size limit is reached
                    if batch_count >= batch_size:
                        try:
                            self.connection.session.execute(batch)
                            loaded_count += batch_count
                            logger.info(f"  Progress: {loaded_count}/{total_records} records inserted into {table_name}")
                            current_batch_records.clear()  # Clear successful batch
                        except Exception as batch_exec_error:
                            error_msg = str(batch_exec_error)
                            
                            # Check if pool is shutdown - if so, stop trying
                            if 'Pool is shutdown' in error_msg or 'shutdown' in error_msg.lower():
                                logger.error(f"Cassandra connection pool shutdown detected. Stopping batch processing.")
                                logger.error(f"Successfully loaded {loaded_count} records before connection failure.")
                                return loaded_count
                            
                            # If batch too large, reduce batch size and retry in smaller chunks
                            if 'Batch too large' in error_msg or 'too large' in error_msg.lower():
                                logger.warning(f"Batch too large ({batch_count} records), splitting into smaller batches")
                                
                                # Retry in smaller chunks (10 records at a time)
                                chunk_size = 10
                                for chunk_idx in range(0, len(current_batch_records), chunk_size):
                                    chunk = current_batch_records[chunk_idx:chunk_idx + chunk_size]
                                    mini_batch = BatchStatement(consistency_level=self.consistency_level)
                                    
                                    for chunk_data in chunk:
                                        chunk_values = tuple(chunk_data.get(col) for col in columns)
                                        mini_batch.add(prepared, chunk_values)
                                    
                                    try:
                                        self.connection.session.execute(mini_batch)
                                        loaded_count += len(chunk)
                                    except Exception as chunk_error:
                                        # If even small batch fails, save for individual retry
                                        logger.debug(f"Small batch failed, saving {len(chunk)} records for individual retry")
                                        failed_records.extend(chunk)
                                
                                current_batch_records.clear()
                                # Reduce batch size for future batches
                                if batch_size > 50:
                                    batch_size = max(50, batch_size // 2)
                                    logger.info(f"Reduced batch size to {batch_size} to avoid 'batch too large' errors")
                            else:
                                logger.error(f"Error executing batch: {error_msg}")
                                errors_in_batch += batch_count
                                current_batch_records.clear()
                        finally:
                            batch = BatchStatement(consistency_level=self.consistency_level)
                            batch_count = 0
                        
                except Exception as e:
                    error_msg = str(e)
                    # Check for connection shutdown
                    if 'Pool is shutdown' in error_msg or 'shutdown' in error_msg.lower():
                        logger.error(f"Cassandra connection pool shutdown detected while adding records.")
                        logger.error(f"Successfully loaded {loaded_count} records before connection failure.")
                        return loaded_count
                    
                    # Log first few errors only to avoid log spam
                    if errors_in_batch < 5:
                        logger.error(f"Error adding record to batch: {e}")
                        logger.debug(f"Record with error: {data}")
                    errors_in_batch += 1
                    # Skip this record and continue with next
                    continue
            
            # Execute remaining batch
            if batch_count > 0:
                try:
                    self.connection.session.execute(batch)
                    loaded_count += batch_count
                    current_batch_records.clear()
                    logger.info(f"  Progress: {loaded_count}/{total_records} records inserted into {table_name} (final batch)")
                except Exception as batch_exec_error:
                    error_msg = str(batch_exec_error)
                    if 'Batch too large' in error_msg or 'too large' in error_msg.lower():
                        logger.warning(f"Final batch too large ({batch_count} records), splitting into smaller batches")
                        
                        # Retry in smaller chunks (10 records at a time)
                        chunk_size = 10
                        for chunk_idx in range(0, len(current_batch_records), chunk_size):
                            chunk = current_batch_records[chunk_idx:chunk_idx + chunk_size]
                            mini_batch = BatchStatement(consistency_level=self.consistency_level)
                            
                            for chunk_data in chunk:
                                chunk_values = tuple(chunk_data.get(col) for col in columns)
                                mini_batch.add(prepared, chunk_values)
                            
                            try:
                                self.connection.session.execute(mini_batch)
                                loaded_count += len(chunk)
                            except Exception as chunk_error:
                                # If even small batch fails, save for individual retry
                                logger.debug(f"Small batch failed, saving {len(chunk)} records for individual retry")
                                failed_records.extend(chunk)
                        
                        current_batch_records.clear()
                    else:
                        logger.error(f"Error executing final batch: {batch_exec_error}")
                        errors_in_batch += batch_count
            
            # Retry failed records individually (one by one)
            if failed_records:
                logger.info(f"Retrying {len(failed_records)} failed records individually...")
                retry_loaded = 0
                retry_failed = 0
                
                for data in failed_records:
                    try:
                        values = tuple(data.get(col) for col in columns)
                        self.connection.session.execute(prepared, values)
                        retry_loaded += 1
                        loaded_count += 1
                        
                        if retry_loaded % 100 == 0:
                            logger.info(f"  Retry progress: {retry_loaded}/{len(failed_records)} records")
                    except Exception as e:
                        retry_failed += 1
                        if retry_failed <= 5:  # Log first 5 errors
                            logger.error(f"Error inserting individual record: {e}")
                
                if retry_loaded > 0:
                    logger.info(f"Successfully retried {retry_loaded}/{len(failed_records)} records individually")
                if retry_failed > 0:
                    logger.warning(f"Failed to retry {retry_failed} records even individually")
                    errors_in_batch = retry_failed  # Update error count
            
            logger.info(f"Loaded {loaded_count}/{len(data_list)} records to {table_name}")
            if errors_in_batch > 0:
                logger.warning(f"Failed to load {errors_in_batch} records due to errors")
            
        except Exception as e:
            logger.error(f"Error in batch load to Cassandra ({entity_name}): {e}")
        
        return loaded_count
    
    def load_batch_async(self, data_list: List[Dict[str, Any]], entity_name: str) -> int:
        """
        Async loading (faster for large volumes)
        
        Args:
            data_list: List of data to load
            entity_name: Entity name
        
        Returns:
            Number of loaded records
        """
        
        if not data_list:
            return 0
        
        table_name = self._get_table_name(entity_name)
        
        if not table_name:
            return 0
        
        try:
            from cassandra.concurrent import execute_concurrent_with_args
            
            # Prepare statement
            columns = list(data_list[0].keys())
            placeholders = ', '.join(['?'] * len(columns))
            columns_str = ', '.join(columns)
            
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            prepared = self._get_prepared_statement(entity_name, query)
            
            # Prepare values
            values_list = [
                tuple(data[col] for col in columns)
                for data in data_list
            ]
            
            # Execute concurrently
            results = execute_concurrent_with_args(
                self.connection.session,
                prepared,
                values_list,
                concurrency=50
            )
            
            # Count successful
            loaded_count = sum(1 for success, _ in results if success)
            
            logger.info(f"Async load: {loaded_count}/{len(data_list)} records to {table_name}")
            
            return loaded_count
            
        except Exception as e:
            logger.error(f"Error in async load to Cassandra ({entity_name}): {e}")
            return 0
    
    def validate_connection(self) -> bool:
        """
        Validates Cassandra connection
        
        Returns:
            True if connection is valid
        """
        try:
            self.connection.session.execute("SELECT now() FROM system.local")
            return True
        except Exception as e:
            logger.error(f"Cassandra connection invalid: {e}")
            return False
    
    def truncate_table(self, entity_name: str) -> bool:
        """
        Truncates a table (useful for complete reload)
        
        Args:
            entity_name: Entity name
        
        Returns:
            True if successful
        """
        
        table_name = self._get_table_name(entity_name)
        
        if not table_name:
            return False
        
        try:
            self.connection.session.execute(f"TRUNCATE {table_name}")
            logger.info(f"Table truncated: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {e}")
            return False
    
    def _get_table_name(self, entity_name: str) -> str:
        """
        Gets table name from mapping
        
        Args:
            entity_name: Entity name
        
        Returns:
            Table name
        """
        mapping = settings.COLLECTION_MAPPINGS.get(entity_name, {})
        return mapping.get('cassandra', '')
    
    def _get_prepared_statement(self, entity_name: str, query: str):
        """
        Gets a prepared statement (with cache)
        
        Args:
            entity_name: Entity name
            query: CQL query
        
        Returns:
            Prepared statement
        """
        
        if entity_name not in self.prepared_statements:
            self.prepared_statements[entity_name] = self.connection.prepare(query)
        
        return self.prepared_statements[entity_name]

