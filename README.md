# MongoDB ETL Migration Tool

Professional ETL (Extract, Transform, Load) system for migrating data from MongoDB to PostgreSQL and Cassandra/ScyllaDB with automatic schema management, robust error handling, and comprehensive logging.

## Features

- **Multi-Database Support:** Migrate to PostgreSQL, Cassandra/ScyllaDB, or both simultaneously
- **Automatic Schema Management:** Auto-creates missing columns and validates schemas
- **Intelligent Batch Processing:** Dynamic batch sizing with automatic retry for failed batches
- **Foreign Key Resolution:** Automatic mapping of MongoDB ObjectIds to relational IDs
- **Encryption Migration:** Supports re-encryption of sensitive data (Salsa20 to AES-256-CBC)
- **Comprehensive Validation:** 12 validation checks covering all migrated tables
- **Comprehensive Logging:** Auto-managed logs with detailed metrics
- **Production Ready:** Robust and reliable data migration

## Quick Start

```bash
# 1. Clone and navigate
cd mongodb-etl-migration

# 2. Setup virtual environment
python3.11 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
# Create .env file with your database credentials

# 5. Run migration
python run_etl.py
```

## Project Structure

```
mongodb-etl-migration/
├── etl/                    # Main ETL package
│   ├── config/            # Configuration and entity mappings
│   ├── extractors/        # MongoDB data extraction
│   ├── transformers/      # Data transformation logic
│   ├── loaders/           # PostgreSQL and Cassandra loaders
│   ├── utils/             # Utilities (connections, logging, etc)
│   └── orchestrator.py    # Main ETL orchestration
│
├── scripts/               # Utility scripts
│   ├── truncate_all_tables.py    # Clean databases
│   └── validate_migration.py     # Validate results
│
├── backups/               # Database schemas and backups
│   └── create_scylla_database.cql
│
├── requirements.txt      # Python dependencies
└── run_etl.py           # Main entry point
```

## Prerequisites

- **Python 3.11** (required for Cassandra driver compatibility)
- **MongoDB 4.0+** (source database)
- **PostgreSQL 12+** (target database)
- **Cassandra 3.11+ or ScyllaDB** (target database, optional)

## Configuration

### Environment Variables

Edit `.env` with your database credentials:

```bash
# MongoDB
MONGODB_URI=mongodb://user:pass@host:port/database?authSource=admin

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_database
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

# Cassandra/ScyllaDB
CASSANDRA_HOSTS=localhost
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=your_keyspace
CASSANDRA_USER=your_cassandra_user
CASSANDRA_PASSWORD=your_cassandra_password
CASSANDRA_DATACENTER=datacenter1

# Encryption
ENCRYPTION_KEY=your_32_byte_key_here
```

### Entity Mappings

Configure entities to migrate in `etl/config/settings.py`:

```python
ENTITY_MAPPINGS = {
    'user': {
        'mongo': 'users',           # MongoDB collection
        'postgres': 'user',         # PostgreSQL table
        'cassandra': 'users',       # Cassandra table
        'strategy': 'dual',         # Load to both databases
        'order': 7,                 # Processing order
        'batch_size': 1000          # Records per batch
    }
}
```

## Usage

### Basic Migration

```bash
# Activate environment
source venv/bin/activate

# Run complete migration
python run_etl.py
```

### Clean and Re-run

```bash
# Clean all tables
python scripts/truncate_all_tables.py

# Run migration
python run_etl.py

# Validate results
python scripts/validate_migration.py
```

### Monitor Progress

```bash
# Watch logs in real-time
tail -f etl/logs/etl_*.log

# Check for errors
tail -f etl/logs/etl_*.log | grep ERROR
```

## Migration Strategy

The ETL follows a strategic approach based on data characteristics:

### Dual Destination (PostgreSQL + Cassandra)
- **role** - Needed by both databases for foreign keys and permissions
- **user** - Core user data accessible from both systems

### PostgreSQL Only
- **Relational data** with foreign keys: province, municipality, parroquia
- **Catalog data**: profession, entities
- **Content tables**: channel, docs, live
- **Many-to-many relationships**: profession_user, entities_user

### Cassandra Only
- **Chat data** optimized for read patterns:
  - room_details (query by room_id)
  - messages_by_room (query messages by room)
  - participants_by_room (query room members)
  - organizations (admin rooms)
- **Lookup tables** for reverse queries:
  - rooms_by_mongo (ObjectId → UUID mapping)
  - rooms_by_user (user's room list)
  - p2p_room_by_users (find P2P rooms)
  - room_by_message (message → room lookup)
  - room_membership_lookup (membership queries)

### Processing Order

The migration respects foreign key dependencies:

1. **Roles** (required by users)
2. **Geographic data** (province → municipality → parroquia)
3. **Catalogs** (profession, entities)
4. **Users** (depends on roles and locations)
5. **Content** (channel, docs, live - depend on users)
6. **Chat data** (rooms → messages → members)
7. **Relationships** (many-to-many tables)

## Key Features

### 1. Automatic Retry System

Failed Cassandra batches are automatically retried individually:

```
Batch too large → Save records → Retry individually → 100% success
```

### 2. Triple-Layer ObjectId Protection

Prevents ObjectId errors through multiple conversion layers:

```
Field Mapper → Custom Transformations → Final Cleanup → No ObjectId errors
```

### 3. Auto Schema Management

Missing columns are automatically detected and added:

```sql
-- Auto-added to PostgreSQL
ALTER TABLE live ADD COLUMN deletedAt TIMESTAMP;

-- Auto-added to Cassandra
ALTER TABLE messages_by_room ADD file_name TEXT;
ALTER TABLE messages_by_room ADD file_size BIGINT;
ALTER TABLE messages_by_room ADD audio_duration INT;
```

### 4. Smart Batch Sizing

Dynamic batch sizes based on data type:

- Messages: 20 records/batch (large content)
- Other entities: 50 records/batch
- Automatically adjusts based on failures

### 5. Clean Log Management

Automatic cleanup before each run:

```
Old logs → Auto-deleted → New run → Clean logs directory
```

Only current run logs are kept for easy review.

### 6. Comprehensive Validation System

The validation script performs 12 exhaustive checks covering 100% of migrated tables:

```bash
python scripts/validate_migration.py
```

**Validation Checks:**

1. **Record Counts** - Verifies counts match between MongoDB and target databases
2. **mongo_id Columns** - Ensures traceability fields exist in all tables
3. **Room Consistency** - Validates room_id relationships in Cassandra
4. **UUID5 Determinism** - Confirms consistent UUID generation
5. **Lookup Tables** - Verifies all 5 Cassandra lookup tables
6. **Organizations** - Validates admin room migration
7. **File URL Replacement** - Ensures all bucket URLs are updated
8. **Message Filtering** - Confirms invalid message types are excluded
9. **Live deletedAt** - Validates soft-delete logic
10. **Foreign Keys** - Checks all 15 foreign key relationships
11. **Data Transformations** - Validates field conversions and formatting
12. **Cross-Database Consistency** - Ensures data integrity across databases

**Coverage:**
- ✅ All PostgreSQL tables
- ✅ All Cassandra tables
- ✅ All foreign key relationships
- ✅ All data transformations
- ✅ All file URL replacements

## Architecture

### ETL Pipeline

```
┌──────────┐     ┌─────────────┐     ┌────────┐
│ MongoDB  │ →   │ Transformer │  →  │ PostgreSQL
│          │     │             │     │
│ Source   │     │ - Mapping   │     └────────┘
│          │     │ - Validation│
└──────────┘     │ - Foreign   │     ┌────────┐
                 │   Keys      │  →  │ Cassandra
                 └─────────────┘     │
                                     └────────┘
```

### Design Patterns

- **Strategy Pattern:** Different loading strategies per entity
- **Factory Pattern:** Database connection management
- **Template Method:** ETL workflow in base classes
- **Singleton:** Database connection instances

## Troubleshooting

### Common Issues

**Issue:** MongoDB connection fails  
**Solution:** Check `MONGODB_URI` format and `authSource` parameter

**Issue:** Batch too large errors  
**Solution:** Already handled! Auto-retry system will process individually

**Issue:** Foreign key not found  
**Solution:** Check entity processing order in `settings.py`

**Issue:** ObjectId errors  
**Solution:** Already handled! Triple-layer conversion system

## Dependencies

Key dependencies (see `requirements.txt` for full list):

```
pymongo>=4.0.0           # MongoDB client
psycopg2-binary>=2.9.0   # PostgreSQL adapter
cassandra-driver>=3.25.0 # Cassandra/ScyllaDB driver
sqlalchemy>=2.0.0        # SQL toolkit
pandas>=2.0.0            # Data manipulation
python-dotenv>=1.0.0     # Environment management
pycryptodome>=3.19.0     # Encryption
```

## Supported Entities

The system currently migrates 15 MongoDB collections to 23 target tables:

### Reference Data (PostgreSQL & Cassandra)
- **role** - User roles (dual destination)
- **province** - Provinces (PostgreSQL)
- **municipality** - Municipalities (PostgreSQL)
- **parroquia** - Parishes (PostgreSQL)
- **profession** - Professional categories (PostgreSQL)
- **entities** - Organizations/entities (PostgreSQL)

### User Data (PostgreSQL & Cassandra)
- **user** - Users (dual destination)
- **channel** - Communication channels (PostgreSQL)
- **docs** - Documents (PostgreSQL)
- **live** - Live streams (PostgreSQL)

### Chat Data (Cassandra Only)
- **room_details** - Chat rooms
- **messages_by_room** - Messages
- **participants_by_room** - Room members
- **organizations** - Admin rooms

### Lookup Tables (Cassandra)
- **rooms_by_mongo** - MongoDB ID to UUID mapping
- **rooms_by_user** - User's room list with denormalized data
- **p2p_room_by_users** - P2P room lookup
- **room_by_message** - Message to room reverse lookup
- **room_membership_lookup** - Membership clustering key lookup

### Relationships (PostgreSQL)
- **profession_user** - Many-to-many user professions
- **entities_user** - Many-to-many user entities

## Security

- Environment variables for credentials (never committed)
- Encryption support for sensitive fields
- Safe handling of personal data
- Connection pooling with timeouts
- SQL injection prevention through parameterized queries

## Performance Tuning

### Batch Sizes

Configure in `etl/config/settings.py`:

```python
'entity_name': {
    'batch_size': 1000,  # Adjust based on record size
    # ...
}
```

### Consistency Levels

Configure Cassandra consistency in `.env`:

```bash
CASSANDRA_WRITE_CONSISTENCY=ONE  # Fastest
CASSANDRA_WRITE_CONSISTENCY=QUORUM  # More safety
```

## Monitoring

### Real-Time

```bash
# Watch migration progress
tail -f etl/logs/etl_*.log

# Monitor only errors
tail -f etl/logs/etl_*.log | grep ERROR
```

### Post-Migration

```bash
# View metrics
cat etl/logs/etl_metrics_*.json | python -m json.tool

# Count records
python scripts/validate_migration.py
```

## License

Private project - All rights reserved by Venqis-NolaTech

## Authors

Venqis-NolaTech Team
