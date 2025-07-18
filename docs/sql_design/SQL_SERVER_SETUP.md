# LME Metal Spreads SQL Server System Setup Guide

## Overview

This guide covers the setup and deployment of the SQL Server-based LME Metal Spreads collection system. The system supports multiple metals (Copper, Aluminum, Zinc, Lead, Nickel, Tin) and provides real-time and historical spread data collection.

## Prerequisites

1. **SQL Server 2016 or later** (Express edition is sufficient for testing)
2. **Python 3.8+** with the following packages:
   - `pyodbc`
   - `blpapi` (Bloomberg Python API)
   - `pandas`
3. **Bloomberg Terminal** running on the same machine or network
4. **ODBC Driver 17 for SQL Server** installed

## Installation Steps

### 1. Database Setup

#### Create Database and Tables

1. Connect to SQL Server using SQL Server Management Studio (SSMS)
2. Execute scripts in order:

```sql
-- 1. Create database and schemas
sqlcmd -S localhost -i sql/schema/01_create_database.sql

-- 2. Create tables and indexes
sqlcmd -S localhost -i sql/schema/02_create_tables.sql

-- 3. Create stored procedures
sqlcmd -S localhost -i sql/procedures/01_insert_procedures.sql

-- 4. Create views
sqlcmd -S localhost -i sql/views/01_market_views.sql
```

### 2. Configuration

1. Copy the example configuration:
```bash
cp config.example.json config.json
```

2. Edit `config.json` with your settings:

```json
{
    "database": {
        "server": "localhost",           // Your SQL Server instance
        "database": "LMEMetalSpreads",   // Database name
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": true,      // Use Windows Authentication
        "username": "",                  // For SQL authentication
        "password": "",                  // For SQL authentication
        "connection_timeout": 30,
        "query_timeout": 300
    },
    "bloomberg": {
        "host": "localhost",             // Bloomberg API host
        "port": 8194                     // Bloomberg API port
    },
    "collection": {
        "batch_size": 50,                // Spreads per Bloomberg request
        "retry_attempts": 3,
        "retry_delay_seconds": 5,
        "duplicate_check": true          // Prevent duplicate ticks
    }
}
```

### 3. Python Environment Setup

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install pyodbc pandas

# Install Bloomberg API (requires Bloomberg Terminal)
# Copy blpapi from Bloomberg installation to your Python site-packages
```

### 4. Test the System

Run the test script to verify setup:

```bash
python scripts/sql_collector/test_sql_system.py
```

Expected output:
```
Testing Database Connection
============================================================
✓ Database connection successful
✓ SQL Server Version: Microsoft SQL Server 2019

Testing Bloomberg Connection
============================================================
✓ Bloomberg session started successfully

Testing Spread Search for CU
============================================================
✓ Found 2564 spreads
```

## Running the System

### 1. Real-time Collection Service

The main service collects data at different frequencies:

```bash
python scripts/sql_collector/realtime_collection_service.py
```

Collection frequencies:
- **REALTIME** (5 min): Active spreads with recent bid/ask
- **REGULAR** (30 min): All spreads market snapshot
- **DAILY** (24 hours): New spread discovery and maintenance

### 2. Manual Data Collection

For one-time or manual collection:

```python
from sql_data_collector import SQLServerDataCollector

collector = SQLServerDataCollector()
collector.connect_database()
collector.start_bloomberg_session()

# Collect copper spreads
spreads = collector.search_spreads('CU')
collector.store_spreads(spreads)

# Get market data
market_data = collector.get_market_data(spreads)
collector.store_tick_data(market_data)

collector.close()
```

### 3. Running as Windows Service

For production deployment, install as Windows Service:

```bash
# Install python-windows-service
pip install pywin32

# Install service (requires admin)
python scripts/sql_collector/install_service.py install

# Start service
sc start LMEMetalSpreadsCollector
```

## Database Maintenance

### 1. Data Retention

Implement data archival for tick_data table:

```sql
-- Archive data older than 90 days
INSERT INTO tick_data_archive
SELECT * FROM market.tick_data
WHERE timestamp < DATEADD(DAY, -90, GETDATE());

DELETE FROM market.tick_data
WHERE timestamp < DATEADD(DAY, -90, GETDATE());
```

### 2. Index Maintenance

Schedule weekly index rebuild:

```sql
-- Rebuild indexes
ALTER INDEX ALL ON market.tick_data REBUILD;
ALTER INDEX ALL ON market.spreads REBUILD;

-- Update statistics
UPDATE STATISTICS market.tick_data;
UPDATE STATISTICS market.spreads;
```

### 3. Backup Strategy

Recommended backup schedule:
- **Full backup**: Weekly
- **Differential backup**: Daily
- **Transaction log backup**: Every 4 hours

```sql
-- Full backup
BACKUP DATABASE LMEMetalSpreads 
TO DISK = 'C:\Backups\LMEMetalSpreads_Full.bak'
WITH FORMAT, COMPRESSION;

-- Differential backup
BACKUP DATABASE LMEMetalSpreads 
TO DISK = 'C:\Backups\LMEMetalSpreads_Diff.bak'
WITH DIFFERENTIAL, COMPRESSION;
```

## Monitoring

### 1. Collection Health Check

Monitor collection status:

```sql
-- Check collection health
SELECT * FROM config.v_collection_health
ORDER BY metal_code, collection_type;

-- Check recent data
SELECT 
    metal_code,
    COUNT(*) as spread_count,
    MAX(last_update_time) as latest_update
FROM market.v_latest_market_data
GROUP BY metal_code;
```

### 2. Performance Monitoring

Key metrics to monitor:
- Tick data insertion rate
- Query response times
- Database size growth
- Bloomberg API response times

### 3. Alerts

Set up SQL Server Agent alerts for:
- Collection service failures
- No data received for > 1 hour
- Database size > threshold
- Failed stored procedures

## Troubleshooting

### Common Issues

1. **Bloomberg Connection Failed**
   - Ensure Bloomberg Terminal is running
   - Check firewall settings for port 8194
   - Verify blpapi installation

2. **Database Connection Failed**
   - Check SQL Server service is running
   - Verify authentication settings
   - Test with `sqlcmd` or SSMS

3. **No Data Collected**
   - Check collection schedules in config.collection_config
   - Verify spreads exist in market.spreads table
   - Check Bloomberg permissions

4. **Duplicate Data**
   - Ensure duplicate_check is enabled in config
   - Check unique constraints on tick_data table
   - Review sp_InsertTickData procedure

### Log Files

Check logs for detailed error information:
- Application logs: `logs/collector.log`
- SQL Server logs: SQL Server Log Viewer
- Windows Event Log: For service errors

## API Access

For programmatic access to the data:

```python
import pyodbc
import pandas as pd

# Connect to database
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=LMEMetalSpreads;"
    "Trusted_Connection=yes;"
)

# Get latest copper spreads
query = """
SELECT * FROM market.v_latest_market_data
WHERE metal_code = 'CU' 
AND data_freshness = 'Active'
ORDER BY todays_volume DESC
"""

df = pd.read_sql(query, conn)
print(df.head())
```

## Support

For issues or questions:
1. Check this documentation
2. Review SQL Server error logs
3. Enable debug logging in config.json
4. Contact system administrator