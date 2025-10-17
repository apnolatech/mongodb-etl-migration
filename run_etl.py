#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main script to run the ETL

Usage:
    # Migrate all entities
    python run_etl.py
    
    # Migrate specific entities
    python run_etl.py --entities user room message
    
    # Migrate only to PostgreSQL
    python run_etl.py --target postgres
    
    # Migrate only to Cassandra
    python run_etl.py --target cassandra
    
    # Migrate to both databases (default)
    python run_etl.py --target both
    
    # Dry-run mode (without loading data)
    python run_etl.py --dry-run
    
    # Sequential mode (no parallelism)
    python run_etl.py --no-parallel
    
    # Force truncate during ETL (not recommended, use truncate_all_tables.py first)
    python run_etl.py --truncate
    
    # Migrate single entity
    python run_etl.py --entity user
"""

import argparse
import sys
from pathlib import Path

# Add root directory to path
sys.path.insert(0, str(Path(__file__).parent))

from etl.orchestrator import ETLOrchestrator
from etl.utils.logger import logger
from etl.config import settings


def parse_arguments():
    """Parse command line arguments"""
    
    parser = argparse.ArgumentParser(
        description='ETL for migration MongoDB -> PostgreSQL + Cassandra',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--entities',
        nargs='+',
        help='List of entities to migrate (e.g., user room message)'
    )
    
    parser.add_argument(
        '--entity',
        type=str,
        help='Migrate a single entity'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry-run mode: extracts and transforms but does not load data'
    )
    
    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        help=f'Batch size (default: {settings.BATCH_SIZE})'
    )
    
    parser.add_argument(
        '--list-entities',
        action='store_true',
        help='List all available entities'
    )
    
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Only validate connections and configuration'
    )
    
    parser.add_argument(
        '--target',
        type=str,
        choices=['postgres', 'cassandra', 'both'],
        default='both',
        help='Target database(s) for migration (default: both)'
    )
    
    parser.add_argument(
        '--truncate',
        action='store_true',
        help='Truncate tables before migration (default: no truncate, run truncate_all_tables.py first)'
    )
    
    return parser.parse_args()


def list_entities():
    """List all configured entities"""
    logger.info("=" * 80)
    logger.info("AVAILABLE ENTITIES FOR MIGRATION")
    logger.info("=" * 80)
    
    for entity_name, mapping in settings.COLLECTION_MAPPINGS.items():
        strategy = mapping.get('strategy', 'unknown')
        mongo = mapping.get('mongo', '-')
        postgres = mapping.get('postgres', '-')
        cassandra = mapping.get('cassandra', '-')
        
        logger.info(f"\n{entity_name}:")
        logger.info(f"  Strategy: {strategy}")
        logger.info(f"  MongoDB: {mongo}")
        logger.info(f"  PostgreSQL: {postgres}")
        logger.info(f"  Cassandra: {cassandra}")
    
    logger.info("\n" + "=" * 80)


def validate_only(target_db: str = 'both'):
    """Only validate configuration and connections"""
    logger.info("=" * 80)
    logger.info("VALIDATING CONFIGURATION AND CONNECTIONS")
    logger.info("=" * 80)
    logger.info(f"Target database: {target_db.upper()}")
    
    try:
        # Validate configuration
        settings.validate()
        logger.info("Configuration valid")
        
        # Initialize and validate connections (without truncating)
        orchestrator = ETLOrchestrator(target_db=target_db, truncate_before=False)
        orchestrator.initialize()
        logger.info("Connections valid")
        
        orchestrator.cleanup()
        
        logger.info("=" * 80)
        logger.info("VALIDATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False


def main():
    """Main function"""
    
    args = parse_arguments()
    
    # List entities
    if args.list_entities:
        list_entities()
        return 0
    
    # Validate
    if args.validate:
        success = validate_only(target_db=args.target)
        return 0 if success else 1
    
    # Configure batch size if specified
    if args.batch_size:
        settings.BATCH_SIZE = args.batch_size
        logger.info(f"Batch size configured: {settings.BATCH_SIZE}")
    
    # Determine entities to migrate
    entities = None
    if args.entity:
        entities = [args.entity]
    elif args.entities:
        entities = args.entities
    
    # Determine parallel mode
    parallel = not args.no_parallel
    
    # Determine truncate mode
    truncate_before = args.truncate
    
    # Show target database info
    logger.info(f"Target database: {args.target.upper()}")
    if truncate_before:
        logger.warning("Truncate mode: ENABLED (tables will be truncated before migration)")
    else:
        logger.info("Truncate mode: DISABLED (run truncate_all_tables.py first if needed)")
    
    # Run ETL
    try:
        orchestrator = ETLOrchestrator(
            dry_run=args.dry_run, 
            target_db=args.target,
            truncate_before=truncate_before
        )
        orchestrator.run(entities=entities, parallel=parallel)
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\nProcess interrupted by user")
        return 130
    
    except Exception as e:
        logger.error(f"Error running ETL: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

