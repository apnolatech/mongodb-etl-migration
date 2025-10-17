#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Complete truncate script for both PostgreSQL and Cassandra
Respects foreign key dependencies
Useful for manual cleanup or when ETL truncate is disabled
"""

import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.utils.database_connections import PostgreSQLConnection, CassandraConnection, CASSANDRA_AVAILABLE
from etl.utils.logger import logger
from sqlalchemy import text

def truncate_postgresql():
    """Truncate all PostgreSQL tables in correct order (children first)"""
    logger.info("=" * 80)
    logger.info("TRUNCATING POSTGRESQL TABLES")
    logger.info("=" * 80)
    
    # Order: children → parents (reverse of migration order)
    tables_in_order = [
        # Many-to-many tables first
        'profession_user', 'entities_user',
        # Children tables
        'live', 'docs',
        # Users
        'user',
        # Location hierarchy
        'parroquia', 'municipality',
        # Independent tables
        'channel', 'profession', 'entities', 'province', 'role',
    ]
    
    try:
        conn = PostgreSQLConnection()
        conn.connect()
        
        logger.info("Truncating tables (respecting foreign key dependencies)...")
        
        for table_name in tables_in_order:
            try:
                logger.info(f"  Truncating '{table_name}'...")
                
                with conn.get_session() as session:
                    # RESTART IDENTITY reinicia las secuencias de auto-increment a 1
                    session.execute(text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE'))
                
                logger.info(f"    SUCCESS: '{table_name}' truncated (ID sequences restarted)")
                
            except Exception as e:
                error_msg = str(e)
                if 'does not exist' in error_msg:
                    logger.warning(f"    - '{table_name}' does not exist, skipping")
                else:
                    logger.error(f"    ERROR: Error truncating '{table_name}': {e}")
        
        conn.close()
        
        logger.info("\nSUCCESS: PostgreSQL truncation completed")
        return True
        
    except Exception as e:
        logger.error(f"Error in PostgreSQL truncation: {e}")
        return False

def truncate_cassandra():
    """Truncate all Cassandra tables"""
    if not CASSANDRA_AVAILABLE:
        logger.warning("Cassandra driver not available, skipping Cassandra truncation")
        return True
    
    logger.info("\n" + "=" * 80)
    logger.info("TRUNCATING CASSANDRA TABLES")
    logger.info("=" * 80)
    
    # Order: children → parents
    tables_in_order = [
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
        'role',
    ]
    
    try:
        conn = CassandraConnection()
        conn.connect()
        
        logger.info("Truncating tables (in dependency order)...")
        
        for table_name in tables_in_order:
            try:
                logger.info(f"  Truncating '{table_name}'...")
                conn.session.execute(f"TRUNCATE {table_name}")
                logger.info(f"    SUCCESS: '{table_name}' truncated")
                
            except Exception as e:
                error_msg = str(e)
                if 'does not exist' in error_msg or 'unconfigured table' in error_msg:
                    logger.warning(f"    - '{table_name}' does not exist, skipping")
                else:
                    logger.error(f"    ERROR: Error truncating '{table_name}': {e}")
        
        conn.close()
        
        logger.info("\nSUCCESS: Cassandra truncation completed")
        return True
        
    except Exception as e:
        logger.error(f"Error in Cassandra truncation: {e}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Truncate PostgreSQL and/or Cassandra tables before ETL migration'
    )
    parser.add_argument(
        '--target',
        type=str,
        choices=['postgres', 'cassandra', 'both'],
        default='both',
        help='Target database(s) to truncate (default: both)'
    )
    
    args = parser.parse_args()
    
    logger.info("\n" + "=" * 80)
    logger.info("DATABASE TRUNCATION")
    logger.info(f"Target: {args.target.upper()}")
    logger.info("This will delete all data from migration tables")
    logger.info("=" * 80)
    
    pg_success = True
    cas_success = True
    
    # Truncate PostgreSQL
    if args.target in ['postgres', 'both']:
        pg_success = truncate_postgresql()
    else:
        logger.info("\nSkipping PostgreSQL (target: {})".format(args.target))
    
    # Truncate Cassandra
    if args.target in ['cassandra', 'both']:
        cas_success = truncate_cassandra()
    else:
        logger.info("\nSkipping Cassandra (target: {})".format(args.target))
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("TRUNCATION SUMMARY")
    logger.info("=" * 80)
    
    if args.target in ['postgres', 'both']:
        logger.info(f"  PostgreSQL: {'SUCCESS' if pg_success else 'FAILED'}")
    
    if args.target in ['cassandra', 'both']:
        logger.info(f"  Cassandra:  {'SUCCESS' if cas_success else 'FAILED'}")
    
    # Determine overall success based on what was attempted
    attempted_success = True
    if args.target in ['postgres', 'both']:
        attempted_success = attempted_success and pg_success
    if args.target in ['cassandra', 'both']:
        attempted_success = attempted_success and cas_success
    
    if attempted_success:
        logger.info("\nSUCCESS: All requested tables truncated successfully")
        logger.info("\nYou can now run the ETL migration:")
        logger.info(f"  python run_etl.py --target {args.target}")
        return 0
    else:
        logger.error("\nERROR: Some truncations failed. Check errors above.")
        logger.info("\nYou can still run ETL for databases that succeeded:")
        if pg_success and not cas_success:
            logger.info("  python run_etl.py --target postgres")
        elif cas_success and not pg_success:
            logger.info("  python run_etl.py --target cassandra")
        return 1

if __name__ == '__main__':
    sys.exit(main())

