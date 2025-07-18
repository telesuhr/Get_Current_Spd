# LME Metal Spreads SQL Server Architecture

## System Overview

The LME Metal Spreads SQL Server system is designed to collect, store, and analyze spread trading data for multiple metals from Bloomberg Terminal. The system supports high-frequency tick data collection while maintaining data integrity and preventing duplicates.

## Architecture Components

```
┌─────────────────────┐     ┌──────────────────────┐
│  Bloomberg Terminal │     │   External Systems   │
│  - Market Data API  │     │  - Dashboard Apps    │
│  - Instrument Search│     │  - Analytics Tools   │
└──────────┬──────────┘     │  - Reporting Systems │
           │                └───────────┬──────────┘
           │ blpapi                     │ SQL/API
           ▼                            ▼
┌──────────────────────────────────────────────────┐
│          Data Collection Layer                   │
│  ┌────────────────────┐  ┌────────────────────┐ │
│  │ SQLDataCollector   │  │ Collection Service │ │
│  │ - Search spreads   │  │ - Scheduler        │ │
│  │ - Get market data  │  │ - Multi-threading  │ │
│  │ - Store ticks      │  │ - Error handling   │ │
│  └────────────────────┘  └────────────────────┘ │
└──────────────────────┬───────────────────────────┘
                       │ pyodbc
                       ▼
┌──────────────────────────────────────────────────┐
│              SQL Server Database                 │
│  ┌────────────────────────────────────────────┐ │
│  │            Configuration Layer              │ │
│  │  - config.metals (CU, AL, ZN, PB, NI, SN) │ │
│  │  - config.collection_config               │ │
│  └────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────┐ │
│  │              Market Data Layer             │ │
│  │  - market.spreads (definitions)           │ │
│  │  - market.tick_data (time series)         │ │
│  │  - market.daily_summary (aggregates)      │ │
│  └────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────┐ │
│  │            Analysis Layer                  │ │
│  │  - Views (latest data, active spreads)    │ │
│  │  - Stored Procedures (calculations)       │ │
│  │  - Functions (date calculations)          │ │
│  └────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────┘
```

## Data Flow

### 1. Spread Discovery
```
Bloomberg API → Search spreads → Classify type → Store in market.spreads
```

### 2. Market Data Collection
```
Active spreads → Request batch → Bloomberg API → Parse response → Store ticks
```

### 3. Data Processing
```
Raw ticks → Duplicate check → Calculate today's volume → Store → Update summaries
```

## Database Schema

### Core Tables

#### market.spreads
- Primary spread definitions
- Indexed by metal_id, ticker, spread_type
- Tracks active/inactive status

#### market.tick_data
- Time-series tick data
- Partitioned by month (optional)
- Indexed for time-based queries

#### market.daily_summary
- Pre-calculated daily statistics
- One row per spread per day
- Used for historical analysis

### Key Design Decisions

1. **Normalization**: Separate tables for metals, spreads, and ticks
2. **Indexing**: Optimized for both real-time queries and historical analysis
3. **Partitioning**: Ready for monthly partitioning of tick_data
4. **Duplicate Prevention**: Multiple levels of duplicate checking

## Collection Strategy

### Three-Tier Collection

1. **Real-time (5 minutes)**
   - Only active spreads (bid/ask in last hour)
   - Minimal Bloomberg API calls
   - Focus on trading hours

2. **Regular (30 minutes)**
   - All registered spreads
   - Complete market snapshot
   - Identify newly active spreads

3. **Daily Maintenance**
   - Search for new spreads
   - Update prompt dates
   - Calculate summaries
   - Archive old data

## Performance Optimizations

### Database Level
- Clustered indexes on time-series data
- Filtered indexes for active data
- Read-committed snapshot isolation
- Statistics auto-update

### Application Level
- Batch processing (50 spreads/request)
- Connection pooling
- Async processing where possible
- Smart duplicate detection

### Query Optimization
- Materialized views for common queries
- Indexed views for real-time data
- Stored procedures for complex logic
- Query hints where beneficial

## Scalability Considerations

### Horizontal Scaling
- Multiple collection services per metal
- Partitioned tables by date/metal
- Read replicas for analytics

### Vertical Scaling
- Increase batch sizes
- Parallel processing
- In-memory tables for hot data
- SSD storage for tick_data

## Security

### Database Security
- Windows Authentication preferred
- Role-based access control
- Encrypted connections
- Audit logging

### Application Security
- Config file encryption
- No hardcoded credentials
- Service account isolation
- API rate limiting

## Monitoring Points

### Health Checks
- Collection service status
- Bloomberg connection health
- Database connection pool
- Data freshness by metal

### Performance Metrics
- Ticks per second inserted
- Query response times
- Bloomberg API latency
- Database growth rate

### Business Metrics
- Active spreads by metal
- Daily volume trends
- Spread type distribution
- Data coverage gaps

## Disaster Recovery

### Backup Strategy
- Full weekly backups
- Daily differentials
- 4-hour transaction logs
- Off-site backup storage

### Recovery Procedures
- Point-in-time recovery
- Partial restore capability
- Collection service failover
- Data validation scripts

## Future Enhancements

### Planned Features
1. Real-time WebSocket feed
2. Machine learning for spread prediction
3. Automated trading signals
4. Cloud migration (Azure SQL)
5. GraphQL API layer

### Extensibility Points
- Additional metal types
- New spread classifications
- Custom field mappings
- Third-party data sources