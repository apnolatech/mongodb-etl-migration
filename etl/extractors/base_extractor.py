# -*- coding: utf-8 -*-
"""
Base extractor for all data sources
"""

from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any, Optional


class BaseExtractor(ABC):
    """Base class for all extractors"""
    
    def __init__(self, batch_size: int = 1000):
        """
        Args:
            batch_size: Batch size for processing records
        """
        self.batch_size = batch_size
    
    @abstractmethod
    def extract(self, entity_name: str, filters: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        """
        Extracts data from source
        
        Args:
            entity_name: Name of the entity to extract
            filters: Optional filters for extraction
        
        Yields:
            Dictionary with extracted data
        """
        pass
    
    @abstractmethod
    def count(self, entity_name: str, filters: Optional[Dict[str, Any]] = None) -> int:
        """
        Counts total records to extract
        
        Args:
            entity_name: Entity name
            filters: Optional filters
        
        Returns:
            Total number of records
        """
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validates that connection is active
        
        Returns:
            True if connection is valid
        """
        pass
