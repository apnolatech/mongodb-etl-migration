#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Migration Validation Script - Enhanced Version
Validates that data was correctly migrated from MongoDB to PostgreSQL and Cassandra
Includes exhaustive checks for data integrity and consistency
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.utils.database_connections import MongoDBConnection, PostgreSQLConnection, CassandraConnection, CASSANDRA_AVAILABLE
from etl.utils.logger import logger
from sqlalchemy import text
from collections import defaultdict
import uuid

class MigrationValidator:
    """Enhanced validator for migration integrity"""
    
    def __init__(self):
        self.mongo = MongoDBConnection()
        self.postgres = PostgreSQLConnection()
        self.cassandra = CassandraConnection() if CASSANDRA_AVAILABLE else None
        self.validation_results = defaultdict(list)
        self.errors = []
        self.warnings = []
        self.stats = defaultdict(dict)
    
    def connect(self):
        """Connect to all databases"""
        logger.info("Connecting to databases...")
        self.mongo.connect()
        self.postgres.connect()
        if self.cassandra:
            try:
                self.cassandra.connect()
            except Exception as e:
                logger.warning(f"Could not connect to Cassandra: {e}")
                self.cassandra = None
    
    def close(self):
        """Close all connections"""
        self.mongo.close()
        self.postgres.close()
        if self.cassandra:
            self.cassandra.close()
    
    def validate_record_counts(self):
        """Validate that record counts match between source and target"""
        logger.info("\n" + "=" * 80)
        logger.info("1. VALIDATING RECORD COUNTS")
        logger.info("=" * 80)
        
        # Define mappings with filters
        mappings = [
            # (mongo_collection, mongo_filter, postgres_table, cassandra_table, expected_postgres, expected_cassandra)
            ('roles', {}, 'role', 'role', True, True),
            ('province', {}, 'province', None, True, False),
            ('municipality', {}, 'municipality', None, True, False),
            ('parroquia', {}, 'parroquia', None, True, False),
            ('professions', {}, 'profession', None, True, False),
            ('entities', {}, 'entities', None, True, False),
            ('users', {'role': {'$in': ['USER_ROLE', 'AGENTE_ROLE']}}, 'user', 'users', True, True),
            ('channel', {}, 'channel', None, True, False),
            ('docs', {}, 'docs', None, True, False),
            ('live', {}, 'live', None, True, False),
            ('rooms', {}, None, 'room_details', False, True),
            ('messages', {}, None, 'messages_by_room', False, True),
            ('roommembers', {}, None, 'participants_by_room', False, True),
        ]
        
        results = []
        
        for mongo_coll, mongo_filter, pg_table, cas_table, check_pg, check_cas in mappings:
            # Get MongoDB count
            try:
                mongo_count = self.mongo.db[mongo_coll].count_documents(mongo_filter)
            except Exception as e:
                mongo_count = 0
                logger.warning(f"Could not count MongoDB collection '{mongo_coll}': {e}")
            
            # Get PostgreSQL count
            pg_count = 0
            if check_pg and pg_table:
                try:
                    with self.postgres.get_session() as session:
                        result = session.execute(text(f'SELECT COUNT(*) FROM "{pg_table}"'))
                        pg_count = result.fetchone()[0]
                except Exception as e:
                    logger.warning(f"Could not count PostgreSQL table '{pg_table}': {e}")
            
            # Get Cassandra count
            cas_count = 0
            if check_cas and cas_table and self.cassandra:
                try:
                    result = self.cassandra.session.execute(f"SELECT COUNT(*) FROM {cas_table}")
                    cas_count = result.one()[0]
                except Exception as e:
                    logger.warning(f"Could not count Cassandra table '{cas_table}': {e}")
            
            # Report
            status = "[OK]"
            if check_pg and pg_count < mongo_count * 0.95:  # Allow 5% tolerance
                status = "[FAIL]"
                self.errors.append(f"{mongo_coll}: MongoDB={mongo_count}, PostgreSQL={pg_count}")
            if check_cas and cas_count < mongo_count * 0.95:
                status = "[FAIL]"
                self.errors.append(f"{mongo_coll}: MongoDB={mongo_count}, Cassandra={cas_count}")
            
            results.append({
                'collection': mongo_coll,
                'mongo': mongo_count,
                'postgres': pg_count if check_pg else '-',
                'cassandra': cas_count if check_cas else '-',
                'status': status
            })
            
            # Store stats
            self.stats['counts'][mongo_coll] = {
                'mongo': mongo_count,
                'postgres': pg_count,
                'cassandra': cas_count
            }
        
        # Print table
        logger.info(f"\n{'Collection':<20} {'MongoDB':>10} {'PostgreSQL':>12} {'Cassandra':>12} {'Status':>8}")
        logger.info("-" * 70)
        for r in results:
            logger.info(f"{r['collection']:<20} {r['mongo']:>10} {str(r['postgres']):>12} {str(r['cassandra']):>12} {r['status']:>8}")
    
    def validate_mongo_id_columns(self):
        """Validate that all tables have mongo_id column for traceability"""
        logger.info("\n" + "=" * 80)
        logger.info("2. VALIDATING mongo_id COLUMNS")
        logger.info("=" * 80)
        
        # PostgreSQL tables
        logger.info("\nPostgreSQL tables:")
        pg_tables = ['role', 'province', 'municipality', 'parroquia', 'profession', 
                     'entities', 'user', 'channel', 'docs', 'live', 'profession_user', 'entities_user']
        
        for table in pg_tables:
            try:
                with self.postgres.get_session() as session:
                    result = session.execute(text(f'SELECT mongo_id FROM "{table}" LIMIT 1'))
                    logger.info(f"  [OK] '{table}' has mongo_id column")
            except Exception as e:
                if 'does not exist' in str(e).lower() or 'column' in str(e).lower():
                    logger.error(f"  [FAIL] '{table}' missing mongo_id column")
                    self.errors.append(f"PostgreSQL table '{table}' missing mongo_id")
                else:
                    logger.warning(f"  [WARN] Could not verify '{table}': {e}")
        
        # Cassandra tables
        if self.cassandra:
            logger.info("\nCassandra tables:")
            cas_tables = ['room_details', 'messages_by_room', 'participants_by_room', 
                         'users', 'role', 'organizations']
            
            for table in cas_tables:
                try:
                    query = f"SELECT mongo_id FROM {table} LIMIT 1"
                    self.cassandra.session.execute(query)
                    logger.info(f"  [OK] '{table}' has mongo_id column")
                except Exception as e:
                    if 'undefined' in str(e).lower() or 'unknown' in str(e).lower():
                        logger.error(f"  [FAIL] '{table}' missing mongo_id column")
                        self.errors.append(f"Cassandra table '{table}' missing mongo_id")
                    else:
                        logger.warning(f"  [WARN] Could not verify '{table}': {e}")
    
    def validate_cassandra_room_consistency(self):
        """EXHAUSTIVE validation of room_id consistency in Cassandra"""
        if not self.cassandra:
            return
        
        logger.info("\n" + "=" * 80)
        logger.info("3. VALIDATING CASSANDRA ROOM CONSISTENCY (EXHAUSTIVE)")
        logger.info("=" * 80)
        
        try:
            # Get ALL distinct room_ids from messages (not just a sample)
            logger.info("\n  Extracting all room_ids from messages_by_room...")
            result = self.cassandra.session.execute("SELECT DISTINCT room_id FROM messages_by_room")
            message_room_ids = set(row.room_id for row in result)
            logger.info(f"  Found {len(message_room_ids)} distinct room_ids in messages")
            
            # Get all room_ids from room_details
            logger.info("\n  Extracting all room_ids from room_details...")
            result = self.cassandra.session.execute("SELECT room_id, mongo_id FROM room_details")
            room_details_data = {row.room_id: row.mongo_id for row in result}
            logger.info(f"  Found {len(room_details_data)} rooms in room_details")
            
            # Check consistency
            logger.info("\n  Checking consistency...")
            missing_in_details = []
            matched = 0
            
            for room_id in message_room_ids:
                if room_id in room_details_data:
                    matched += 1
                else:
                    missing_in_details.append(room_id)
            
            logger.info(f"  [OK] Matched: {matched}/{len(message_room_ids)}")
            logger.info(f"  {'[FAIL]' if missing_in_details else '[OK]'} Missing in room_details: {len(missing_in_details)}")
            
            if missing_in_details:
                logger.error(f"\n  ERROR: {len(missing_in_details)} room_ids in messages NOT found in room_details")
                logger.error("  This indicates room_id generation inconsistency!")
                self.errors.append(f"{len(missing_in_details)} orphaned room_ids in messages")
                
                # Show sample
                logger.error("  Sample of orphaned room_ids (first 10):")
                for room_id in missing_in_details[:10]:
                    logger.error(f"    - {room_id}")
            else:
                logger.info("  [OK] SUCCESS: All room_ids in messages exist in room_details!")
            
            # Store stats
            self.stats['room_consistency'] = {
                'total_message_rooms': len(message_room_ids),
                'total_room_details': len(room_details_data),
                'matched': matched,
                'missing': len(missing_in_details)
            }
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR validating room consistency: {e}")
            self.errors.append(f"Room consistency check failed: {e}")
    
    def validate_uuid5_determinism(self):
        """Validate that UUID5 generation is deterministic"""
        if not self.cassandra:
            return
        
        logger.info("\n" + "=" * 80)
        logger.info("4. VALIDATING UUID5 DETERMINISM")
        logger.info("=" * 80)
        
        try:
            # Get sample of rooms and verify their UUIDs
            result = self.cassandra.session.execute("SELECT room_id, mongo_id FROM room_details LIMIT 20")
            rooms = list(result)
            
            errors = 0
            for row in rooms:
                if row.mongo_id:
                    # Generate UUID5 from mongo_id
                    expected_uuid = uuid.uuid5(uuid.NAMESPACE_OID, str(row.mongo_id))
                    
                    if row.room_id == expected_uuid:
                        logger.info(f"  [OK] mongo_id {row.mongo_id} -> UUID {row.room_id}")
                    else:
                        logger.error(f"  [FAIL] ERROR: mongo_id {row.mongo_id}")
                        logger.error(f"            Expected: {expected_uuid}")
                        logger.error(f"            Got:      {row.room_id}")
                        errors += 1
                        self.errors.append(f"UUID5 mismatch for mongo_id {row.mongo_id}")
            
            if errors == 0:
                logger.info(f"\n  [OK] SUCCESS: All {len(rooms)} sampled UUIDs are correctly generated")
            else:
                logger.error(f"\n  [FAIL] FAILURE: {errors}/{len(rooms)} UUIDs are incorrect")
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR checking UUID5 determinism: {e}")
            self.errors.append(f"UUID5 check failed: {e}")
    
    def validate_rooms_by_mongo(self):
        """Validate rooms_by_mongo lookup table"""
        if not self.cassandra:
            return
        
        logger.info("\n" + "=" * 80)
        logger.info("5. VALIDATING rooms_by_mongo LOOKUP TABLE")
        logger.info("=" * 80)
        
        try:
            # Get count of rooms_by_mongo
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM rooms_by_mongo")
            lookup_count = result.one()[0]
            
            # Get count of room_details
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM room_details")
            room_count = result.one()[0]
            
            logger.info(f"  rooms_by_mongo records: {lookup_count}")
            logger.info(f"  room_details records: {room_count}")
            
            if lookup_count == room_count:
                logger.info(f"  [OK] SUCCESS: rooms_by_mongo count matches room_details")
            else:
                logger.warning(f"  [WARN] WARNING: Mismatch - room_details: {room_count}, rooms_by_mongo: {lookup_count}")
                self.warnings.append(f"rooms_by_mongo count mismatch: {lookup_count} vs {room_count}")
            
            # Verify sample lookups work
            result = self.cassandra.session.execute("SELECT mongo_id, room_id FROM rooms_by_mongo LIMIT 5")
            logger.info("\n  Sample lookups:")
            for row in result:
                logger.info(f"    mongo_id={row.mongo_id} -> room_id={row.room_id}")
            
            self.stats['rooms_by_mongo'] = {
                'count': lookup_count,
                'expected': room_count
            }
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR checking rooms_by_mongo: {e}")
            self.errors.append(f"rooms_by_mongo check failed: {e}")
    
    def validate_lookup_tables(self):
        """Validate all Cassandra lookup tables"""
        if not self.cassandra:
            return
        
        logger.info("\n" + "=" * 80)
        logger.info("6. VALIDATING CASSANDRA LOOKUP TABLES")
        logger.info("=" * 80)
        
        try:
            # p2p_room_by_users
            logger.info("\n  p2p_room_by_users:")
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM p2p_room_by_users")
            p2p_count = result.one()[0]
            logger.info(f"    Records: {p2p_count}")
            
            if p2p_count > 0:
                logger.info(f"    [OK] Table populated")
                result = self.cassandra.session.execute("SELECT user1_id, user2_id, room_id FROM p2p_room_by_users LIMIT 3")
                logger.info("    Sample entries:")
                for row in result:
                    logger.info(f"      user1_id={row.user1_id}, user2_id={row.user2_id}, room_id={row.room_id}")
            else:
                logger.warning(f"    [WARN] Table is empty - check if there are P2P rooms")
                self.warnings.append("p2p_room_by_users is empty")
            
            # rooms_by_user
            logger.info("\n  rooms_by_user:")
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM rooms_by_user")
            rbu_count = result.one()[0]
            logger.info(f"    Records: {rbu_count}")
            
            # Should match participants_by_room count
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM participants_by_room")
            participants_count = result.one()[0]
            
            if rbu_count == participants_count:
                logger.info(f"    [OK] Count matches participants_by_room ({participants_count})")
            else:
                logger.warning(f"    [WARN] Count mismatch: {rbu_count} vs participants_by_room: {participants_count}")
                self.warnings.append(f"rooms_by_user count mismatch: {rbu_count} vs {participants_count}")
            
            # room_by_message
            logger.info("\n  room_by_message:")
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM room_by_message")
            rbm_count = result.one()[0]
            logger.info(f"    Records: {rbm_count}")
            
            # Should match messages_by_room count
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM messages_by_room")
            messages_count = result.one()[0]
            
            if rbm_count == messages_count:
                logger.info(f"    [OK] Count matches messages_by_room ({messages_count})")
            else:
                logger.warning(f"    [WARN] Count mismatch: {rbm_count} vs messages_by_room: {messages_count}")
                self.warnings.append(f"room_by_message count mismatch: {rbm_count} vs {messages_count}")
            
            # room_membership_lookup
            logger.info("\n  room_membership_lookup:")
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM room_membership_lookup")
            rml_count = result.one()[0]
            logger.info(f"    Records: {rml_count}")
            
            if rml_count == participants_count:
                logger.info(f"    [OK] Count matches participants_by_room ({participants_count})")
            else:
                logger.warning(f"    [WARN] Count mismatch: {rml_count} vs participants_by_room: {participants_count}")
                self.warnings.append(f"room_membership_lookup count mismatch: {rml_count} vs {participants_count}")
            
            # Check role field is populated
            result = self.cassandra.session.execute("SELECT role FROM room_membership_lookup LIMIT 10")
            null_roles = sum(1 for row in result if not row.role)
            if null_roles == 0:
                logger.info(f"    [OK] All sampled records have role field")
            else:
                logger.error(f"    [FAIL] {null_roles}/10 sampled records missing role")
                self.errors.append("room_membership_lookup has NULL roles")
            
            self.stats['lookup_tables'] = {
                'p2p_room_by_users': p2p_count,
                'rooms_by_user': rbu_count,
                'room_by_message': rbm_count,
                'room_membership_lookup': rml_count
            }
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR checking lookup tables: {e}")
            self.errors.append(f"Lookup tables check failed: {e}")
    
    def validate_organizations(self):
        """Validate organizations table"""
        if not self.cassandra:
            return
        
        logger.info("\n" + "=" * 80)
        logger.info("7. VALIDATING ORGANIZATIONS TABLE")
        logger.info("=" * 80)
        
        try:
            # Get organizations count
            result = self.cassandra.session.execute("SELECT COUNT(*) FROM organizations")
            org_count = result.one()[0]
            logger.info(f"  Cassandra organizations: {org_count}")
            
            # Get MongoDB count of rooms with isAdmin=true
            mongo_org_count = self.mongo.db.rooms.count_documents({"isAdmin": True})
            logger.info(f"  MongoDB isAdmin=true rooms: {mongo_org_count}")
            
            if org_count == mongo_org_count:
                logger.info(f"  [OK] SUCCESS: All organizations migrated ({org_count}/{mongo_org_count})")
            else:
                logger.warning(f"  [WARN] WARNING: Count mismatch - Expected {mongo_org_count}, got {org_count}")
                self.warnings.append(f"Organizations count mismatch: {org_count} vs {mongo_org_count}")
            
            # Check that organizations have mongo_id
            result = self.cassandra.session.execute("SELECT room_id, mongo_id FROM organizations LIMIT 10")
            null_mongo_id = 0
            for row in result:
                if row.mongo_id is None:
                    null_mongo_id += 1
            
            if null_mongo_id == 0:
                logger.info(f"  [OK] SUCCESS: All organizations have mongo_id populated")
            else:
                logger.error(f"  [FAIL] ERROR: {null_mongo_id}/10 sampled organizations have NULL mongo_id")
                self.errors.append("Organizations missing mongo_id")
            
            self.stats['organizations'] = {
                'cassandra': org_count,
                'mongodb': mongo_org_count
            }
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR checking organizations: {e}")
            self.errors.append(f"Organizations check failed: {e}")
    
    def validate_file_url_replacement(self):
        """Validate that ALL file URLs have bucket replacement applied"""
        logger.info("\n" + "=" * 80)
        logger.info("8. VALIDATING FILE URL REPLACEMENT")
        logger.info("=" * 80)
        
        # PostgreSQL
        logger.info("\nPostgreSQL:")
        try:
            with self.postgres.get_session() as session:
                # User avatars
                result = session.execute(text('SELECT COUNT(*) FROM "user" WHERE avatar LIKE \'gs://sibci%\''))
                old_avatars = result.fetchone()[0]
                logger.info(f"  {'[OK]' if old_avatars == 0 else '[FAIL]'} user.avatar: {old_avatars} with gs://sibci*")
                if old_avatars > 0:
                    self.errors.append(f"{old_avatars} users with unreplaced avatar URLs")
                
                # User avatar_thumbnail
                result = session.execute(text('SELECT COUNT(*) FROM "user" WHERE avatar_thumbnail LIKE \'gs://sibci%\''))
                old_thumb = result.fetchone()[0]
                logger.info(f"  {'[OK]' if old_thumb == 0 else '[FAIL]'} user.avatar_thumbnail: {old_thumb} with gs://sibci*")
                if old_thumb > 0:
                    self.errors.append(f"{old_thumb} users with unreplaced avatar_thumbnail URLs")
                
                # Channel pictures
                result = session.execute(text('SELECT COUNT(*) FROM channel WHERE picture LIKE \'gs://sibci%\''))
                old_pictures = result.fetchone()[0]
                logger.info(f"  {'[OK]' if old_pictures == 0 else '[FAIL]'} channel.picture: {old_pictures} with gs://sibci*")
                if old_pictures > 0:
                    self.errors.append(f"{old_pictures} channels with unreplaced picture URLs")
                
                # Channel covers
                result = session.execute(text('SELECT COUNT(*) FROM channel WHERE cover LIKE \'gs://sibci%\''))
                old_covers = result.fetchone()[0]
                logger.info(f"  {'[OK]' if old_covers == 0 else '[FAIL]'} channel.cover: {old_covers} with gs://sibci*")
                if old_covers > 0:
                    self.errors.append(f"{old_covers} channels with unreplaced cover URLs")
                
                # Docs fileURL
                result = session.execute(text('SELECT COUNT(*) FROM docs WHERE "fileURL" LIKE \'gs://sibci%\''))
                old_docs = result.fetchone()[0]
                logger.info(f"  {'[OK]' if old_docs == 0 else '[FAIL]'} docs.fileURL: {old_docs} with gs://sibci*")
                if old_docs > 0:
                    self.errors.append(f"{old_docs} docs with unreplaced fileURL")
                
                # Live photo_url
                result = session.execute(text('SELECT COUNT(*) FROM live WHERE photo_url LIKE \'gs://sibci%\''))
                old_photos = result.fetchone()[0]
                logger.info(f"  {'[OK]' if old_photos == 0 else '[FAIL]'} live.photo_url: {old_photos} with gs://sibci*")
                if old_photos > 0:
                    self.errors.append(f"{old_photos} lives with unreplaced photo_url")
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR checking PostgreSQL URLs: {e}")
        
        # Cassandra
        if self.cassandra:
            logger.info("\nCassandra:")
            try:
                # User avatars
                result = self.cassandra.session.execute("SELECT COUNT(*) FROM users WHERE avatar LIKE 'gs://sibci%' ALLOW FILTERING")
                old_cas_avatars = result.one()[0]
                logger.info(f"  {'[OK]' if old_cas_avatars == 0 else '[FAIL]'} users.avatar: {old_cas_avatars} with gs://sibci*")
                if old_cas_avatars > 0:
                    self.errors.append(f"{old_cas_avatars} Cassandra users with unreplaced avatars")
                
                # Room images
                result = self.cassandra.session.execute("SELECT COUNT(*) FROM room_details WHERE image LIKE 'gs://sibci%' ALLOW FILTERING")
                old_images = result.one()[0]
                logger.info(f"  {'[OK]' if old_images == 0 else '[FAIL]'} room_details.image: {old_images} with gs://sibci*")
                if old_images > 0:
                    self.errors.append(f"{old_images} rooms with unreplaced image URLs")
                
                # Message file_url
                result = self.cassandra.session.execute("SELECT COUNT(*) FROM messages_by_room WHERE file_url LIKE 'gs://sibci%' ALLOW FILTERING")
                old_files = result.one()[0]
                logger.info(f"  {'[OK]' if old_files == 0 else '[FAIL]'} messages_by_room.file_url: {old_files} with gs://sibci*")
                if old_files > 0:
                    self.errors.append(f"{old_files} messages with unreplaced file_url")
                
                # Message file_thumbnail_url
                result = self.cassandra.session.execute("SELECT COUNT(*) FROM messages_by_room WHERE file_thumbnail_url LIKE 'gs://sibci%' ALLOW FILTERING")
                old_thumbs = result.one()[0]
                logger.info(f"  {'[OK]' if old_thumbs == 0 else '[FAIL]'} messages_by_room.file_thumbnail_url: {old_thumbs} with gs://sibci*")
                if old_thumbs > 0:
                    self.errors.append(f"{old_thumbs} messages with unreplaced file_thumbnail_url")
            
            except Exception as e:
                logger.error(f"  [FAIL] ERROR checking Cassandra URLs: {e}")
    
    def validate_message_filtering(self):
        """Validate that filtered message types are not present"""
        if not self.cassandra:
            return
        
        logger.info("\n" + "=" * 80)
        logger.info("9. VALIDATING MESSAGE FILTERING")
        logger.info("=" * 80)
        
        try:
            # Check for filtered message types
            invalid_types = ['GEO_LOCATION', 'SURVEY', 'CONTACT']
            
            for msg_type in invalid_types:
                result = self.cassandra.session.execute(f"SELECT COUNT(*) FROM messages_by_room WHERE type = '{msg_type}' ALLOW FILTERING")
                count = result.one()[0]
                logger.info(f"  {'[OK]' if count == 0 else '[FAIL]'} type='{msg_type}': {count} found (should be 0)")
                if count > 0:
                    self.errors.append(f"{count} messages with type '{msg_type}' found (should be filtered)")
            
            # Note: Cannot easily check msgLife or isActive in Cassandra after migration
            logger.info(f"  [OK] Message type filtering validated")
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR checking message filtering: {e}")
            self.errors.append(f"Message filtering check failed: {e}")
    
    def validate_live_deleted_at(self):
        """Validate that inactive lives have deleted_at set"""
        logger.info("\n" + "=" * 80)
        logger.info("10. VALIDATING LIVE deleted_at FIELD")
        logger.info("=" * 80)
        
        try:
            with self.postgres.get_session() as session:
                # Check if deleted_at is populated for inactive lives
                result = session.execute(text('SELECT COUNT(*) FROM live WHERE deleted_at IS NOT NULL'))
                deleted_lives = result.fetchone()[0]
                logger.info(f"  Lives with deleted_at: {deleted_lives}")
                
                if deleted_lives > 0:
                    logger.info(f"  [OK] {deleted_lives} inactive lives have deleted_at set")
                else:
                    logger.info(f"  [INFO] No inactive lives found (or all are active)")
                
                # Verify all lives have proper fields
                result = session.execute(text('SELECT COUNT(*) FROM live'))
                total_lives = result.fetchone()[0]
                logger.info(f"  Total lives: {total_lives}")
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR checking live deleted_at: {e}")
    
    def validate_foreign_keys(self):
        """Validate foreign key relationships in PostgreSQL"""
        logger.info("\n" + "=" * 80)
        logger.info("11. VALIDATING FOREIGN KEY RELATIONSHIPS")
        logger.info("=" * 80)
        
        checks = [
            ('municipality', 'province_id', 'province', 'id'),
            ('parroquia', 'province_id', 'province', 'id'),
            ('parroquia', 'municipality_id', 'municipality', 'id'),
            ('user', 'province_id', 'province', 'id'),
            ('user', 'municipality_id', 'municipality', 'id'),
            ('user', 'parroquia_id', 'parroquia', 'id'),
            ('user', 'roleId', 'role', 'id'),
            ('channel', 'created_by', 'user', 'id'),
            ('live', 'channel_id', 'channel', 'id'),
            ('live', 'created_by', 'user', 'id'),
            ('docs', 'uploaded_by_id', 'user', 'id'),
            ('profession_user', 'user_id', 'user', 'id'),
            ('profession_user', 'profession_id', 'profession', 'id'),
            ('entities_user', 'user_id', 'user', 'id'),
            ('entities_user', 'entities_id', 'entities', 'id'),
        ]
        
        for child_table, child_column, parent_table, parent_column in checks:
            try:
                with self.postgres.get_session() as session:
                    # Check for orphaned records
                    query = text(f'''
                        SELECT COUNT(*) 
                        FROM "{child_table}" c
                        WHERE c."{child_column}" IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1 FROM "{parent_table}" p 
                            WHERE p."{parent_column}" = c."{child_column}"
                        )
                    ''')
                    result = session.execute(query)
                    orphaned = result.fetchone()[0]
                    
                    if orphaned == 0:
                        logger.info(f"  [OK] {child_table}.{child_column} -> {parent_table}.{parent_column}")
                    else:
                        logger.error(f"  [FAIL] {child_table}.{child_column} -> {parent_table}.{parent_column}: {orphaned} orphaned")
                        self.errors.append(f"{child_table}.{child_column} has {orphaned} orphaned records")
            
            except Exception as e:
                logger.warning(f"  [WARN] Could not verify {child_table}.{child_column}: {e}")
    
    def validate_data_transformations(self):
        """Validate specific data transformations"""
        logger.info("\n" + "=" * 80)
        logger.info("12. VALIDATING DATA TRANSFORMATIONS")
        logger.info("=" * 80)
        
        # User transformations
        logger.info("\nUser transformations:")
        try:
            with self.postgres.get_session() as session:
                # profileStatus
                result = session.execute(text('''
                    SELECT COUNT(*) FROM "user" WHERE "profileStatus" != 'COMPLETED'
                '''))
                non_completed = result.fetchone()[0]
                logger.info(f"  {'[OK]' if non_completed == 0 else '[WARN]'} profileStatus='COMPLETED': {non_completed} exceptions")
                
                # DNI format
                result = session.execute(text('''
                    SELECT COUNT(*) FROM "user" 
                    WHERE dni IS NOT NULL AND dni != '' AND dni NOT LIKE '%-%'
                '''))
                invalid_dni = result.fetchone()[0]
                logger.info(f"  {'[OK]' if invalid_dni == 0 else '[WARN]'} DNI format (V-00000000): {invalid_dni} without hyphen")
                
                # Avatar URLs
                result = session.execute(text('''
                    SELECT COUNT(*) FROM "user" 
                    WHERE avatar LIKE '%gs://sibci%'
                '''))
                old_urls = result.fetchone()[0]
                logger.info(f"  {'[OK]' if old_urls == 0 else '[WARN]'} Avatar URL replacement: {old_urls} still with gs://sibci*")
                
                # roleId population
                result = session.execute(text('SELECT COUNT(*) FROM "user" WHERE "roleId" IS NULL'))
                null_roles = result.fetchone()[0]
                logger.info(f"  {'[OK]' if null_roles == 0 else '[FAIL]'} roleId populated: {null_roles} NULL values")
                if null_roles > 0:
                    self.errors.append(f"{null_roles} users missing roleId")
                
                # Deleted users
                result = session.execute(text('''
                    SELECT COUNT(*) FROM "user" WHERE deleted_at IS NOT NULL
                '''))
                deleted_count = result.fetchone()[0]
                
                result = session.execute(text('''
                    SELECT COUNT(*) FROM "user" 
                    WHERE deleted_at IS NOT NULL 
                    AND name = 'Deleted User' 
                    AND email = '' 
                    AND phone = ''
                    AND deleted_name IS NOT NULL
                '''))
                valid_deleted = result.fetchone()[0]
                
                logger.info(f"  [OK] Deleted users (isActive=false): {deleted_count} total")
                logger.info(f"    {'[OK]' if valid_deleted == deleted_count else '[FAIL]'} Properly formatted: {valid_deleted}/{deleted_count}")
                
                if valid_deleted != deleted_count:
                    self.errors.append(f"{deleted_count - valid_deleted} deleted users improperly formatted")
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR validating user transformations: {e}")
        
        # Live transformations
        logger.info("\nLive transformations:")
        try:
            with self.postgres.get_session() as session:
                result = session.execute(text('SELECT COUNT(*) FROM live'))
                total_lives = result.fetchone()[0]
                
                # live_type
                result = session.execute(text('SELECT COUNT(*) FROM live WHERE live_type = \'YOUTUBE\''))
                youtube = result.fetchone()[0]
                logger.info(f"  {'[OK]' if youtube == total_lives else '[WARN]'} live_type='YOUTUBE': {youtube}/{total_lives}")
                
                # status
                result = session.execute(text('SELECT COUNT(*) FROM live WHERE status IN (\'LIVE\', \'FINISHED\')'))
                valid_status = result.fetchone()[0]
                logger.info(f"  {'[OK]' if valid_status == total_lives else '[WARN]'} status (LIVE/FINISHED): {valid_status}/{total_lives}")
                
                # category
                result = session.execute(text('SELECT COUNT(*) FROM live WHERE category IN (\'FEATURED\', \'MORE_TO_SEE\')'))
                valid_category = result.fetchone()[0]
                logger.info(f"  {'[OK]' if valid_category == total_lives else '[WARN]'} category (FEATURED/MORE_TO_SEE): {valid_category}/{total_lives}")
                
                # live_url
                result = session.execute(text('SELECT COUNT(*) FROM live WHERE live_url IS NOT NULL AND live_url != \'\''))
                with_url = result.fetchone()[0]
                logger.info(f"  {'[OK]' if with_url == total_lives else '[WARN]'} live_url populated: {with_url}/{total_lives}")
        
        except Exception as e:
            logger.error(f"  [FAIL] ERROR validating live transformations: {e}")
        
        # Cassandra transformations
        if self.cassandra:
            logger.info("\nCassandra transformations:")
            try:
                # Messages type
                result = self.cassandra.session.execute("SELECT type FROM messages_by_room LIMIT 100")
                types = [row.type for row in result]
                user_msg = sum(1 for t in types if t == 'user_message')
                logger.info(f"  [OK] messages type='user_message': {user_msg}/100 sample")
                
                # Participants role uppercase
                result = self.cassandra.session.execute("SELECT role FROM participants_by_room LIMIT 100")
                roles = [row.role for row in result]
                uppercase = sum(1 for r in roles if r and r.isupper())
                logger.info(f"  {'[OK]' if uppercase == len(roles) else '[WARN]'} participants role UPPERCASE: {uppercase}/100 sample")
            
            except Exception as e:
                logger.error(f"  [FAIL] ERROR validating Cassandra transformations: {e}")
    
    def generate_summary(self):
        """Generate validation summary"""
        logger.info("\n" + "=" * 80)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 80)
        
        total_checks = 12  # Updated with new validation sections
        failed_checks = len([e for e in self.errors if 'check failed' in e])
        
        logger.info(f"\nChecks completed: {total_checks}")
        logger.info(f"Errors found: {len(self.errors)}")
        logger.info(f"Warnings: {len(self.warnings)}")
        
        if self.errors:
            logger.error("\n[ERROR] ERRORS:")
            for error in self.errors[:20]:  # Show first 20
                logger.error(f"  - {error}")
            if len(self.errors) > 20:
                logger.error(f"  ... and {len(self.errors) - 20} more errors")
        
        if self.warnings:
            logger.warning("\n[WARN]️  WARNINGS:")
            for warning in self.warnings[:10]:  # Show first 10
                logger.warning(f"  - {warning}")
            if len(self.warnings) > 10:
                logger.warning(f"  ... and {len(self.warnings) - 10} more warnings")
        
        # Key statistics
        if self.stats:
            logger.info("\n[STATS] KEY STATISTICS:")
            
            if 'counts' in self.stats:
                total_mongo = sum(v['mongo'] for v in self.stats['counts'].values())
                total_pg = sum(v['postgres'] for v in self.stats['counts'].values() if isinstance(v['postgres'], int))
                total_cas = sum(v['cassandra'] for v in self.stats['counts'].values() if isinstance(v['cassandra'], int))
                logger.info(f"  Total records - MongoDB: {total_mongo:,}, PostgreSQL: {total_pg:,}, Cassandra: {total_cas:,}")
            
            if 'room_consistency' in self.stats:
                rc = self.stats['room_consistency']
                logger.info(f"  Room consistency - Matched: {rc['matched']}, Missing: {rc['missing']}")
            
            if 'organizations' in self.stats:
                org = self.stats['organizations']
                logger.info(f"  Organizations - MongoDB: {org['mongodb']}, Cassandra: {org['cassandra']}")
        
        # Final verdict
        logger.info("\n" + "=" * 80)
        if len(self.errors) == 0:
            logger.info("✅ SUCCESS: Migration validation PASSED")
            logger.info("=" * 80)
            return 0
        else:
            logger.error("[ERROR] FAILURE: Migration validation FAILED")
            logger.error(f"Please review the {len(self.errors)} errors above")
            logger.error("=" * 80)
            return 1
    
    def run(self):
        """Run all validations"""
        try:
            self.connect()
            
            logger.info("\n" + "=" * 80)
            logger.info("ENHANCED MIGRATION VALIDATION")
            logger.info("=" * 80)
            
            # Run all validations
            self.validate_record_counts()
            self.validate_mongo_id_columns()
            self.validate_cassandra_room_consistency()
            self.validate_uuid5_determinism()
            self.validate_rooms_by_mongo()
            self.validate_lookup_tables()  # NEW
            self.validate_organizations()
            self.validate_file_url_replacement()  # NEW
            self.validate_message_filtering()  # NEW
            self.validate_live_deleted_at()  # NEW
            self.validate_foreign_keys()
            self.validate_data_transformations()
            
            # Generate summary
            return self.generate_summary()
        
        except Exception as e:
            logger.error(f"Fatal error during validation: {e}")
            import traceback
            traceback.print_exc()
            return 1
        
        finally:
            self.close()

def main():
    """Main entry point"""
    validator = MigrationValidator()
    return validator.run()

if __name__ == '__main__':
    sys.exit(main())
