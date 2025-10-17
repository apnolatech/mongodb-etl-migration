# -*- coding: utf-8 -*-
"""
Base transformer
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseTransformer(ABC):
    """Base class for all transformers"""
    
    @abstractmethod
    def transform(self, source_data: Dict[str, Any], entity_name: str) -> Dict[str, Any]:
        """
        Transforms data from source format to target format
        
        Args:
            source_data: Data in source format
            entity_name: Entity name
        
        Returns:
            Transformed data
        """
        pass
    
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validates that transformed data is correct
        
        Args:
            data: Data to validate
        
        Returns:
            True if data is valid
        """
        pass
