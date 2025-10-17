# -*- coding: utf-8 -*-
"""
MongoDB data extractor
"""

from typing import Iterator, Dict, Any, Optional
from bson import ObjectId

from etl.extractors.base_extractor import BaseExtractor
from etl.utils.database_connections import MongoDBConnection
from etl.utils.logger import logger
from etl.utils.validators import validator
from etl.config import settings


class MongoExtractor(BaseExtractor):
    """Extracts data from MongoDB"""
    
    def __init__(self, connection: MongoDBConnection, batch_size: int = None):
        """
        Args:
            connection: MongoDB connection
            batch_size: Batch size (uses global configuration if not specified)
        """
        super().__init__(batch_size or settings.BATCH_SIZE)
        self.connection = connection
    
    def extract(self, entity_name: str, filters: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        """
        Extracts documents from a MongoDB collection
        
        Args:
            entity_name: Entity name (maps to collection)
            filters: Optional MongoDB filters
        
        Yields:
            MongoDB document converted to dictionary
        """
        
        # Get collection name from mapping
        mapping = settings.COLLECTION_MAPPINGS.get(entity_name)
        if not mapping or 'mongo' not in mapping:
            logger.error(f"MongoDB mapping not found for entity: {entity_name}")
            return
        
        collection_name = mapping['mongo']
        logger.info(f"Extracting from MongoDB: {collection_name}")
        
        try:
            collection = self.connection.get_collection(collection_name)
            
            # Build query
            query = filters or {}
            
            # Add entity-specific filters
            if entity_name == 'message':
                # Skip messages with invalid types or states
                # Based on Go migration rules:
                # - MsgType must not be GEO_LOCATION, SURVEY, or CONTACT
                # - MsgLife must not be TOUCH_SHOW_DELETE
                # - isActive must not be false
                if 'msgType' not in query:
                    query['msgType'] = {'$nin': ['GEO_LOCATION', 'SURVEY', 'CONTACT']}
                
                if 'msgLife' not in query:
                    query['msgLife'] = {'$ne': 'TOUCH_SHOW_DELETE'}
                
                if 'isActive' not in query:
                    query['$or'] = [
                        {'isActive': {'$ne': False}},
                        {'isActive': {'$exists': False}}
                    ]
            
            # Count total documents
            total = collection.count_documents(query)
            logger.info(f"Total documents to extract: {total:,}")
            
            # Extract in batches
            extracted = 0
            cursor = collection.find(query).batch_size(self.batch_size)
            
            for document in cursor:
                # Convert ObjectId to string
                document = self._serialize_document(document)
                
                # Apply validation based on entity type
                if settings.ENABLE_VALIDATION:
                    is_valid, error_msg = self._validate_document(entity_name, document)
                    if not is_valid:
                        logger.debug(f"Skipping invalid {entity_name}: {error_msg}")
                        continue
                
                yield document
                
                extracted += 1
                if extracted % self.batch_size == 0:
                    logger.debug(f"Extracted {extracted:,}/{total:,} documents")
            
            logger.info(f"Extraction completed: {extracted:,} documents")
            
        except Exception as e:
            logger.error(f"Error extracting from {collection_name}: {e}")
            raise
    
    def _validate_document(self, entity_name: str, document: Dict[str, Any]) -> tuple:
        """
        Validates document based on entity type using business rules from Go code
        
        Args:
            entity_name: Name of the entity
            document: Document to validate
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        
        # Apply specific validations based on entity type
        if entity_name == 'user':
            return validator.validate_user(document)
        
        elif entity_name == 'room':
            return validator.validate_room(document)
        
        elif entity_name == 'message':
            return validator.validate_message(document)
        
        elif entity_name == 'room_member':
            room_type = document.get('roomType') or 'group'
            return validator.validate_room_member(document, room_type)
        
        elif entity_name == 'docs':
            return validator.validate_document(document)
        
        elif entity_name in ['province', 'municipality', 'parroquia']:
            return validator.validate_location_entity(document, entity_name)
        
        # No specific validation for this entity
        return True, None
    
    def extract_with_aggregation(self, entity_name: str, pipeline: list) -> Iterator[Dict[str, Any]]:
        """
        Extracts data using an aggregation pipeline
        
        Args:
            entity_name: Entity name
            pipeline: MongoDB aggregation pipeline
        
        Yields:
            Document resulting from aggregation
        """
        
        mapping = settings.COLLECTION_MAPPINGS.get(entity_name)
        if not mapping or 'mongo' not in mapping:
            logger.error(f"MongoDB mapping not found for entity: {entity_name}")
            return
        
        collection_name = mapping['mongo']
        
        try:
            collection = self.connection.get_collection(collection_name)
            cursor = collection.aggregate(pipeline, allowDiskUse=True)
            
            for document in cursor:
                yield self._serialize_document(document)
                
        except Exception as e:
            logger.error(f"Error in aggregation for {collection_name}: {e}")
            raise
    
    def count(self, entity_name: str, filters: Optional[Dict[str, Any]] = None) -> int:
        """
        Counts documents in a collection
        
        Args:
            entity_name: Entity name
            filters: Optional filters
        
        Returns:
            Number of documents
        """
        
        mapping = settings.COLLECTION_MAPPINGS.get(entity_name)
        if not mapping or 'mongo' not in mapping:
            return 0
        
        collection_name = mapping['mongo']
        
        try:
            collection = self.connection.get_collection(collection_name)
            return collection.count_documents(filters or {})
        except Exception as e:
            logger.error(f"Error counting documents in {collection_name}: {e}")
            return 0
    
    def validate_connection(self) -> bool:
        """
        Validates MongoDB connection
        
        Returns:
            True if connection is valid
        """
        try:
            self.connection.client.server_info()
            return True
        except Exception as e:
            logger.error(f"MongoDB connection invalid: {e}")
            return False
    
    def _serialize_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serializes a MongoDB document (converts ObjectId, datetime, etc)
        
        Args:
            document: MongoDB document
        
        Returns:
            Serialized document
        """
        if not document:
            return document
        
        serialized = {}
        
        for key, value in document.items():
            if isinstance(value, ObjectId):
                serialized[key] = str(value)
            elif isinstance(value, dict):
                serialized[key] = self._serialize_document(value)
            elif isinstance(value, list):
                serialized[key] = [
                    self._serialize_document(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                serialized[key] = value
        
        return serialized
    
    def extract_by_ids(self, entity_name: str, ids: list) -> Iterator[Dict[str, Any]]:
        """
        Extracts specific documents by their IDs
        
        Args:
            entity_name: Entity name
            ids: List of IDs to extract
        
        Yields:
            MongoDB document
        """
        
        mapping = settings.COLLECTION_MAPPINGS.get(entity_name)
        if not mapping or 'mongo' not in mapping:
            return
        
        collection_name = mapping['mongo']
        
        try:
            collection = self.connection.get_collection(collection_name)
            
            # Convert IDs to ObjectId if necessary
            object_ids = []
            for id_val in ids:
                if isinstance(id_val, str):
                    try:
                        object_ids.append(ObjectId(id_val))
                    except:
                        object_ids.append(id_val)
                else:
                    object_ids.append(id_val)
            
            # Find documents
            cursor = collection.find({'_id': {'$in': object_ids}})
            
            for document in cursor:
                yield self._serialize_document(document)
                
        except Exception as e:
            logger.error(f"Error extracting by IDs from {collection_name}: {e}")
            raise

