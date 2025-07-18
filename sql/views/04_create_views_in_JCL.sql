-- Views for JCL Database LME System
-- Version: 3.0
-- Date: 2025-07-18

-- Ensure you are connected to JCL database before running

-- Drop existing views if they exist
IF OBJECT_ID('lme_market.v_latest_market_data', 'V') IS NOT NULL DROP VIEW lme_market.v_latest_market_data;
IF OBJECT_ID('lme_market.v_active_spreads', 'V') IS NOT NULL DROP VIEW lme_market.v_active_spreads;
IF OBJECT_ID('lme_market.v_todays_activity', 'V') IS NOT NULL DROP VIEW lme_market.v_todays_activity;
IF OBJECT_ID('lme_market.v_spread_type_summary', 'V') IS NOT NULL DROP VIEW lme_market.v_spread_type_summary;
IF OBJECT_ID('lme_market.v_price_history', 'V') IS NOT NULL DROP VIEW lme_market.v_price_history;
IF OBJECT_ID('lme_market.v_daily_summary', 'V') IS NOT NULL DROP VIEW lme_market.v_daily_summary;
IF OBJECT_ID('lme_config.v_collection_health', 'V') IS NOT NULL DROP VIEW lme_config.v_collection_health;
GO

-- View for latest market data (most recent tick for each spread)
CREATE VIEW lme_market.v_latest_market_data
AS
WITH LatestTicks AS (
    SELECT 
        spread_id,
        MAX(timestamp) as latest_timestamp
    FROM lme_market.LME_T_tick_data
    WHERE timestamp > DATEADD(HOUR, -24, GETDATE())  -- Last 24 hours
    GROUP BY spread_id
)
SELECT 
    m.metal_code,
    m.metal_name,
    s.ticker,
    s.spread_type,
    s.description,
    s.prompt_date1,
    s.prompt_date2,
    s.leg1_description,
    s.leg2_description,
    t.bid,
    t.ask,
    t.last_price,
    t.bid_size,
    t.ask_size,
    t.todays_volume,
    t.open_interest,
    (t.ask - t.bid) as bid_ask_spread,
    t.timestamp as last_update_time,
    t.last_update_dt,
    t.trading_dt,
    CASE 
        WHEN t.timestamp > DATEADD(MINUTE, -10, GETDATE()) THEN 'Active'
        WHEN t.timestamp > DATEADD(HOUR, -1, GETDATE()) THEN 'Recent'
        ELSE 'Stale'
    END as data_freshness
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
JOIN LatestTicks lt ON s.spread_id = lt.spread_id
JOIN lme_market.LME_T_tick_data t ON t.spread_id = lt.spread_id AND t.timestamp = lt.latest_timestamp
WHERE s.is_active = 1;
GO

-- View for active spreads (with bid/ask in last hour)
CREATE VIEW lme_market.v_active_spreads
AS
SELECT 
    m.metal_code,
    s.ticker,
    s.spread_type,
    MAX(t.timestamp) as last_quote_time,
    MAX(CASE WHEN t.bid IS NOT NULL THEN t.timestamp END) as last_bid_time,
    MAX(CASE WHEN t.ask IS NOT NULL THEN t.timestamp END) as last_ask_time,
    MAX(t.todays_volume) as max_todays_volume,
    COUNT(DISTINCT CAST(t.timestamp AS DATE)) as days_active,
    AVG(t.ask - t.bid) as avg_spread
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
JOIN lme_market.LME_T_tick_data t ON s.spread_id = t.spread_id
WHERE t.timestamp > DATEADD(HOUR, -1, GETDATE())
AND (t.bid IS NOT NULL OR t.ask IS NOT NULL)
GROUP BY m.metal_code, s.ticker, s.spread_type;
GO

-- View for today's trading activity
CREATE VIEW lme_market.v_todays_activity
AS
SELECT 
    m.metal_code,
    s.ticker,
    s.spread_type,
    MIN(t.last_price) as days_low,
    MAX(t.last_price) as days_high,
    MAX(t.todays_volume) as volume,
    COUNT(DISTINCT t.timestamp) as tick_count,
    MIN(t.timestamp) as first_tick_time,
    MAX(t.timestamp) as last_tick_time
FROM lme_market.LME_T_tick_data t
JOIN lme_market.LME_M_spreads s ON t.spread_id = s.spread_id
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
WHERE CAST(t.timestamp AS DATE) = CAST(GETDATE() AS DATE)
AND t.todays_volume > 0
GROUP BY m.metal_code, s.ticker, s.spread_type;
GO

-- View for spread type summary
CREATE VIEW lme_market.v_spread_type_summary
AS
SELECT 
    m.metal_code,
    s.spread_type,
    COUNT(DISTINCT s.spread_id) as spread_count,
    COUNT(DISTINCT CASE WHEN t.timestamp > DATEADD(HOUR, -1, GETDATE()) 
                       THEN s.spread_id END) as active_count,
    SUM(CASE WHEN t.timestamp > DATEADD(HOUR, -24, GETDATE()) 
             THEN t.todays_volume ELSE 0 END) as volume_24h
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
LEFT JOIN lme_market.LME_T_tick_data t ON s.spread_id = t.spread_id
WHERE s.is_active = 1
GROUP BY m.metal_code, s.spread_type;
GO

-- View for historical price data (for charting)
CREATE VIEW lme_market.v_price_history
AS
SELECT 
    s.spread_id,
    m.metal_code,
    s.ticker,
    t.timestamp,
    t.bid,
    t.ask,
    t.last_price,
    (t.bid + t.ask) / 2.0 as mid_price,
    t.volume,
    t.todays_volume
FROM lme_market.LME_T_tick_data t
JOIN lme_market.LME_M_spreads s ON t.spread_id = s.spread_id
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
WHERE t.last_price IS NOT NULL 
   OR (t.bid IS NOT NULL AND t.ask IS NOT NULL);
GO

-- View for daily summary data
CREATE VIEW lme_market.v_daily_summary
AS
SELECT 
    m.metal_code,
    s.ticker,
    s.spread_type,
    ds.trading_date,
    ds.open_price,
    ds.high_price,
    ds.low_price,
    ds.close_price,
    ds.vwap,
    ds.total_volume,
    ds.trade_count,
    ds.avg_bid_ask_spread,
    (ds.close_price - ds.open_price) as daily_change,
    CASE 
        WHEN ds.open_price > 0 
        THEN ((ds.close_price - ds.open_price) / ds.open_price) * 100 
        ELSE NULL 
    END as daily_change_pct
FROM lme_market.LME_T_daily_summary ds
JOIN lme_market.LME_M_spreads s ON ds.spread_id = s.spread_id
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id;
GO

-- View for monitoring data collection health
CREATE VIEW lme_config.v_collection_health
AS
WITH RecentData AS (
    SELECT 
        s.metal_id,
        s.spread_type,
        COUNT(DISTINCT s.spread_id) as total_spreads,
        COUNT(DISTINCT CASE WHEN t.timestamp > DATEADD(MINUTE, -30, GETDATE()) 
                           THEN s.spread_id END) as recent_updates
    FROM lme_market.LME_M_spreads s
    LEFT JOIN lme_market.LME_T_tick_data t ON s.spread_id = t.spread_id
    WHERE s.is_active = 1
    GROUP BY s.metal_id, s.spread_type
)
SELECT 
    m.metal_code,
    m.metal_name,
    rd.spread_type,
    rd.total_spreads,
    rd.recent_updates,
    CAST(rd.recent_updates AS FLOAT) / NULLIF(rd.total_spreads, 0) * 100 as update_percentage,
    cc.collection_type,
    cc.interval_minutes,
    cc.last_run,
    DATEDIFF(MINUTE, cc.last_run, GETDATE()) as minutes_since_last_run,
    CASE 
        WHEN cc.last_run IS NULL THEN 'Never Run'
        WHEN DATEDIFF(MINUTE, cc.last_run, GETDATE()) > cc.interval_minutes * 2 THEN 'Overdue'
        WHEN DATEDIFF(MINUTE, cc.last_run, GETDATE()) > cc.interval_minutes THEN 'Due'
        ELSE 'OK'
    END as collection_status
FROM lme_config.LME_M_metals m
JOIN RecentData rd ON m.metal_id = rd.metal_id
LEFT JOIN lme_config.LME_M_collection_config cc ON m.metal_id = cc.metal_id
WHERE m.is_active = 1;
GO

-- Verify view creation
SELECT 
    s.name AS SchemaName,
    v.name AS ViewName,
    v.create_date
FROM sys.views v
JOIN sys.schemas s ON v.schema_id = s.schema_id
WHERE s.name IN ('lme_market', 'lme_config')
ORDER BY s.name, v.name;

PRINT '';
PRINT 'LME views created successfully in JCL database';
PRINT '';
PRINT 'All database objects have been created successfully!';
PRINT '';
PRINT 'To verify the installation, run:';
PRINT 'SELECT * FROM lme_config.LME_M_metals;';
PRINT 'SELECT * FROM lme_config.v_collection_health;';