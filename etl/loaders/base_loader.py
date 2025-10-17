# -*- coding: utf-8 -*-
"""
Base loader
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseLoader(ABC):
    """Base class for all loaders"""
    
    @abstractmethod
    def load(self, data: Dict[str, Any], entity_name: str) -> bool:
        """
        Loads a record to the target
        
        Args:
            data: Data to load
            entity_name: Entity name
        
        Returns:
            True if loading was successful
        """
        pass
    
    @abstractmethod
    def load_batch(self, data_list: List[Dict[str, Any]], entity_name: str) -> int:
        """
        Loads a batch of records
        
        Args:
            data_list: List of data to load
            entity_name: Entity name
        
        Returns:
            Number of successfully loaded records
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
