# -*- coding: utf-8 -*-
"""
Metrics system for ETL
"""

import time
import json
from typing import Dict, Any
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

from etl.config import settings
from etl.utils.logger import logger


@dataclass
class EntityMetrics:
    """Metrics for a specific entity"""
    
    entity_name: str
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded_postgres: int = 0
    records_loaded_cassandra: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: float = None
    duration_seconds: float = 0
    
    def finish(self):
        """Marks the entity as finished"""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
    
    def to_dict(self) -> Dict[str, Any]:
        """Converts to dictionary"""
        return asdict(self)


@dataclass
class ETLMetrics:
    """General ETL metrics"""
    
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime = None
    duration_seconds: float = 0
    
    total_entities: int = 0
    entities_success: int = 0
    entities_failed: int = 0
    
    total_records_extracted: int = 0
    total_records_transformed: int = 0
    total_records_loaded_postgres: int = 0
    total_records_loaded_cassandra: int = 0
    total_errors: int = 0
    
    entities_metrics: Dict[str, EntityMetrics] = field(default_factory=dict)
    
    def add_entity_metrics(self, entity_metrics: EntityMetrics):
        """Adds metrics for an entity"""
        self.entities_metrics[entity_metrics.entity_name] = entity_metrics
        
        # Update totals
        self.total_records_extracted += entity_metrics.records_extracted
        self.total_records_transformed += entity_metrics.records_transformed
        self.total_records_loaded_postgres += entity_metrics.records_loaded_postgres
        self.total_records_loaded_cassandra += entity_metrics.records_loaded_cassandra
        self.total_errors += entity_metrics.errors
        
        if entity_metrics.errors == 0:
            self.entities_success += 1
        else:
            self.entities_failed += 1
    
    def finish(self):
        """Marks the ETL as finished"""
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Converts to dictionary"""
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration_seconds,
            'total_entities': self.total_entities,
            'entities_success': self.entities_success,
            'entities_failed': self.entities_failed,
            'total_records_extracted': self.total_records_extracted,
            'total_records_transformed': self.total_records_transformed,
            'total_records_loaded_postgres': self.total_records_loaded_postgres,
            'total_records_loaded_cassandra': self.total_records_loaded_cassandra,
            'total_errors': self.total_errors,
            'entities_metrics': {
                name: metrics.to_dict() 
                for name, metrics in self.entities_metrics.items()
            }
        }
    
    def save_to_file(self, filename: str = None):
        """Saves metrics to a JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"etl_metrics_{timestamp}.json"
        
        filepath = settings.LOGS_DIR / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
        
        logger.info(f"Metrics saved to: {filepath}")
    
    def print_summary(self):
        """Prints a summary of metrics"""
        logger.info("=" * 80)
        logger.info("ETL MIGRATION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Start: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End: {self.end_time.strftime('%Y-%m-%d %H:%M:%S') if self.end_time else 'In progress'}")
        logger.info(f"Duration: {self.duration_seconds:.2f} seconds")
        logger.info("")
        logger.info(f"Total entities: {self.total_entities}")
        logger.info(f"  - Successful: {self.entities_success}")
        logger.info(f"  - Failed: {self.entities_failed}")
        logger.info("")
        logger.info(f"Records extracted: {self.total_records_extracted:,}")
        logger.info(f"Records transformed: {self.total_records_transformed:,}")
        logger.info(f"Records loaded to PostgreSQL: {self.total_records_loaded_postgres:,}")
        logger.info(f"Records loaded to Cassandra: {self.total_records_loaded_cassandra:,}")
        logger.info(f"Total errors: {self.total_errors:,}")
        logger.info("")
        logger.info("Detail by entity:")
        logger.info("-" * 80)
        
        for entity_name, metrics in self.entities_metrics.items():
            logger.info(f"\n{entity_name}:")
            logger.info(f"  Extracted: {metrics.records_extracted:,}")
            logger.info(f"  Transformed: {metrics.records_transformed:,}")
            if metrics.records_loaded_postgres > 0:
                logger.info(f"  Loaded to PostgreSQL: {metrics.records_loaded_postgres:,}")
            if metrics.records_loaded_cassandra > 0:
                logger.info(f"  Loaded to Cassandra: {metrics.records_loaded_cassandra:,}")
            logger.info(f"  Errors: {metrics.errors}")
            logger.info(f"  Duration: {metrics.duration_seconds:.2f}s")
        
        logger.info("=" * 80)

