# -*- coding: utf-8 -*-
"""
Data extractors from MongoDB
"""

from .base_extractor import BaseExtractor
from .mongo_extractor import MongoExtractor

__all__ = ['BaseExtractor', 'MongoExtractor']
