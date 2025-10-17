# -*- coding: utf-8 -*-
"""
PostgreSQL data loader
"""

from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
import io

from etl.loaders.base_loader import BaseLoader
from etl.utils.database_connections import PostgreSQLConnection
from etl.utils.logger import logger
from etl.config import settings


class PostgresLoader(BaseLoader):
    """Loads data to PostgreSQL"""
    
    def __init__(self, connection: PostgreSQLConnection, id_mapper=None):
        """
        Args:
            connection: PostgreSQL connection
            id_mapper: Optional ID mapper to update cache after inserts
        """
        self.connection = connection
        self.id_mapper = id_mapper
    
    def load(self, data: Dict[str, Any], entity_name: str) -> bool:
        """
        Loads a record to PostgreSQL
        
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
            
            with self.connection.get_session() as session:
                # Use INSERT ... ON CONFLICT DO UPDATE (UPSERT)
                stmt = insert(text(table_name)).values(**data)
                
                # Configure ON CONFLICT (assuming 'id' is primary key)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id'],
                    set_=data
                )
                
                session.execute(stmt)
                
            return True
            
        except Exception as e:
            logger.error(f"Error loading record to PostgreSQL ({entity_name}): {e}")
            logger.debug(f"Data: {data}")
            return False
    
    def load_batch(self, data_list: List[Dict[str, Any]], entity_name: str) -> int:
        """
        Loads a batch of records to PostgreSQL
        
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
        
        # Build insertion query
        columns = list(data_list[0].keys())
        placeholders = ', '.join([f':{col}' for col in columns])
        # Quote column names for case-sensitivity
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        # Base query for bulk insert (without RETURNING for speed)
        query = f"""
            INSERT INTO "{table_name}" ({columns_str})
            VALUES ({placeholders})
        """
        
        # Use bulk insert for better performance (all in single transaction)
        total_records = len(data_list)
        
        try:
            # Use COPY FROM for maximum performance
            loaded_count = self._bulk_copy(table_name, columns, data_list)
            
            if loaded_count > 0:
                logger.info(f"  Progress: {total_records}/{total_records} records inserted into {table_name} (COPY)")
            
            # Update ID mapper cache - query back the inserted records
            if self.id_mapper and len(data_list) <= 5000:  # Only for reasonable sizes
                try:
                    with self.connection.get_session() as session:
                        mongo_ids = [d.get('mongo_id') for d in data_list if d.get('mongo_id')]
                        if mongo_ids:
                            # Query in larger chunks for speed
                            chunk_size = 1000
                            for i in range(0, len(mongo_ids), chunk_size):
                                chunk = mongo_ids[i:i + chunk_size]
                                placeholders_list = ','.join([f"'{mid}'" for mid in chunk])
                                query_ids = text(f'SELECT id, mongo_id FROM "{table_name}" WHERE mongo_id IN ({placeholders_list})')
                                result = session.execute(query_ids)
                                for row in result:
                                    self.id_mapper.add_to_cache(table_name, row.mongo_id, row.id)
                except Exception as cache_error:
                    pass  # Cache update is optional
                    
        except Exception as e:
            # If bulk insert fails, fall back to individual inserts
            error_msg = str(e).split('\n')[0]
            logger.warning(f"Bulk insert failed: {error_msg}. Falling back to individual inserts...")
            
            query_single = f"""
                INSERT INTO "{table_name}" ({columns_str})
                VALUES ({placeholders})
                RETURNING id, mongo_id
            """
            
            for idx, data in enumerate(data_list, 1):
                try:
                    with self.connection.get_session() as session:
                        result = session.execute(text(query_single), data)
                        
                        if self.id_mapper and result:
                            row = result.fetchone()
                            if row:
                                self.id_mapper.add_to_cache(table_name, row.mongo_id, row.id)
                        
                    loaded_count += 1
                    
                    if idx % 200 == 0 or idx == total_records:
                        logger.info(f"  Progress: {idx}/{total_records} records (fallback mode)")
                        
                except Exception as e:
                    if loaded_count == 0:
                        logger.error(f"Error: {str(e).split(chr(10))[0]}")
                        logger.debug(f"Data: {data}")
        
        logger.info(f"Loaded {loaded_count}/{len(data_list)} records to {table_name}")
        
        return loaded_count
    
    def _bulk_copy(self, table_name: str, columns: List[str], data_list: List[Dict[str, Any]]) -> int:
        """
        Uses PostgreSQL COPY FROM for ultra-fast bulk insert
        
        Args:
            table_name: Table name
            columns: List of column names
            data_list: List of records
            
        Returns:
            Number of records loaded
        """
        from datetime import datetime
        
        try:
            # Get raw psycopg2 connection
            conn = self.connection.engine.raw_connection()
            cursor = conn.cursor()
            
            # Create CSV in memory
            output = io.StringIO()
            
            for record in data_list:
                values = []
                for col in columns:
                    val = record.get(col)
                    
                    # Convert None to \N (PostgreSQL NULL in COPY)
                    if val is None:
                        values.append('\\N')
                    # Convert datetime to string
                    elif isinstance(val, datetime):
                        values.append(val.isoformat())
                    # Convert boolean to string
                    elif isinstance(val, bool):
                        values.append('t' if val else 'f')
                    # Escape special characters
                    else:
                        val_str = str(val).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                        values.append(val_str)
                
                # Write tab-separated row
                output.write('\t'.join(values) + '\n')
            
            # Rewind to start
            output.seek(0)
            
            # Execute COPY FROM
            columns_str = ', '.join([f'"{col}"' for col in columns])
            cursor.copy_expert(
                f'COPY "{table_name}" ({columns_str}) FROM STDIN WITH (FORMAT text, NULL \'\\N\')',
                output
            )
            
            conn.commit()
            cursor.close()
            
            return len(data_list)
            
        except Exception as e:
            logger.debug(f"COPY FROM failed: {e}")
            raise
    
    def load_batch_bulk(self, data_list: List[Dict[str, Any]], entity_name: str) -> int:
        """
        Bulk load using COPY (faster for large volumes)
        
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
            import io
            import csv
            
            # Create CSV in memory
            output = io.StringIO()
            columns = list(data_list[0].keys())
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writerows(data_list)
            output.seek(0)
            
            # Use COPY
            with self.connection.engine.raw_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.copy_expert(
                        f"COPY {table_name} ({','.join(columns)}) FROM STDIN WITH CSV",
                        output
                    )
                conn.commit()
            
            logger.info(f"Bulk load successful: {len(data_list)} records to {table_name}")
            return len(data_list)
            
        except Exception as e:
            logger.error(f"Error in bulk load to PostgreSQL ({entity_name}): {e}")
            return 0
    
    def validate_connection(self) -> bool:
        """
        Validates PostgreSQL connection
        
        Returns:
            True if connection is valid
        """
        try:
            with self.connection.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"PostgreSQL connection invalid: {e}")
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
            with self.connection.get_session() as session:
                session.execute(text(f'TRUNCATE TABLE "{table_name}" CASCADE'))
                session.commit()
            
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
        return mapping.get('postgres', '')

