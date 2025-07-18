-- LME Metal Spreads Tables for JCL Database
-- Version: 3.0
-- Date: 2025-07-18
-- Creates tables with LME_ prefix in JCL database

-- Ensure you are connected to JCL database before running

-- Drop tables if they exist (in correct order due to foreign keys)
IF OBJECT_ID('lme_market.LME_T_tick_data', 'U') IS NOT NULL DROP TABLE lme_market.LME_T_tick_data;
IF OBJECT_ID('lme_market.LME_T_daily_summary', 'U') IS NOT NULL DROP TABLE lme_market.LME_T_daily_summary;
IF OBJECT_ID('lme_market.LME_M_spreads', 'U') IS NOT NULL DROP TABLE lme_market.LME_M_spreads;
IF OBJECT_ID('lme_config.LME_M_metals', 'U') IS NOT NULL DROP TABLE lme_config.LME_M_metals;
IF OBJECT_ID('lme_config.LME_M_collection_config', 'U') IS NOT NULL DROP TABLE lme_config.LME_M_collection_config;
GO

-- ===================================
-- Master Tables (M_ prefix with LME_)
-- ===================================

-- Metal master table
CREATE TABLE lme_config.LME_M_metals (
    metal_id INT IDENTITY(1,1) PRIMARY KEY,
    metal_code NVARCHAR(10) NOT NULL UNIQUE,  -- e.g., 'CU', 'AL', 'ZN'
    metal_name NVARCHAR(50) NOT NULL,         -- e.g., 'Copper', 'Aluminum'
    bloomberg_base NVARCHAR(20) NOT NULL,     -- e.g., 'LMCADS' for copper
    exchange_code NVARCHAR(10) DEFAULT 'LME',
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- Collection configuration master
CREATE TABLE lme_config.LME_M_collection_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    metal_id INT NOT NULL,
    collection_type NVARCHAR(20) NOT NULL,    -- 'REALTIME', 'REGULAR', 'DAILY'
    interval_minutes INT NOT NULL,
    is_active BIT DEFAULT 1,
    last_run DATETIME2,
    next_run DATETIME2,
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (metal_id) REFERENCES lme_config.LME_M_metals(metal_id)
);

-- Spread definitions master
CREATE TABLE lme_market.LME_M_spreads (
    spread_id INT IDENTITY(1,1) PRIMARY KEY,
    metal_id INT NOT NULL,
    ticker NVARCHAR(50) NOT NULL,
    spread_type NVARCHAR(20) NOT NULL,        -- 'Calendar', '3M-3W', 'Odd-Odd', etc.
    description NVARCHAR(255),
    prompt_date1 DATE,
    prompt_date2 DATE,
    leg1_description NVARCHAR(100),           -- e.g., '2025-08-20'
    leg2_description NVARCHAR(100),           -- e.g., '2025-09-17'
    is_active BIT DEFAULT 1,
    first_seen_date DATE DEFAULT CAST(GETDATE() AS DATE),
    last_seen_date DATE DEFAULT CAST(GETDATE() AS DATE),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    UNIQUE(metal_id, ticker),
    FOREIGN KEY (metal_id) REFERENCES lme_config.LME_M_metals(metal_id)
);

-- ===================================
-- Transaction Tables (T_ prefix with LME_)
-- ===================================

-- Tick data (main transaction table)
CREATE TABLE lme_market.LME_T_tick_data (
    tick_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    spread_id INT NOT NULL,
    timestamp DATETIME2(3) NOT NULL,
    bid DECIMAL(12,4),
    ask DECIMAL(12,4),
    last_price DECIMAL(12,4),
    bid_size INT,
    ask_size INT,
    volume BIGINT,
    todays_volume BIGINT,
    open_interest INT,
    last_update_dt DATE,
    trading_dt DATE,
    rt_spread_bp DECIMAL(10,2),              -- Real-time spread in basis points
    contract_value DECIMAL(18,2),
    data_source NVARCHAR(20) DEFAULT 'Bloomberg',
    quality_flag TINYINT DEFAULT 0,          -- 0=good, 1=stale, 2=suspect
    created_at DATETIME2(3) DEFAULT GETDATE(),
    FOREIGN KEY (spread_id) REFERENCES lme_market.LME_M_spreads(spread_id)
);

-- Daily summary table
CREATE TABLE lme_market.LME_T_daily_summary (
    summary_id INT IDENTITY(1,1) PRIMARY KEY,
    spread_id INT NOT NULL,
    trading_date DATE NOT NULL,
    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    vwap DECIMAL(12,4),                      -- Volume Weighted Average Price
    total_volume BIGINT,
    trade_count INT,
    avg_bid_ask_spread DECIMAL(10,4),
    max_bid_ask_spread DECIMAL(10,4),
    time_with_quotes INT,                    -- Minutes with valid quotes
    created_at DATETIME2 DEFAULT GETDATE(),
    UNIQUE(spread_id, trading_date),
    FOREIGN KEY (spread_id) REFERENCES lme_market.LME_M_spreads(spread_id)
);

-- ===================================
-- Indexes for Performance
-- ===================================

-- Indexes on master tables
CREATE INDEX IX_LME_M_spreads_metal_type ON lme_market.LME_M_spreads(metal_id, spread_type) WHERE is_active = 1;
CREATE INDEX IX_LME_M_spreads_prompt_dates ON lme_market.LME_M_spreads(prompt_date1, prompt_date2);
CREATE INDEX IX_LME_M_spreads_ticker ON lme_market.LME_M_spreads(ticker);

-- Indexes on transaction tables
CREATE INDEX IX_LME_T_tick_timestamp ON lme_market.LME_T_tick_data(timestamp) INCLUDE (spread_id);
CREATE INDEX IX_LME_T_tick_spread_timestamp ON lme_market.LME_T_tick_data(spread_id, timestamp DESC);
CREATE INDEX IX_LME_T_tick_todays_volume ON lme_market.LME_T_tick_data(todays_volume) WHERE todays_volume > 0;
CREATE INDEX IX_LME_T_tick_trading_dt ON lme_market.LME_T_tick_data(trading_dt, spread_id);

CREATE INDEX IX_LME_T_daily_date ON lme_market.LME_T_daily_summary(trading_date DESC);
CREATE INDEX IX_LME_T_daily_spread_date ON lme_market.LME_T_daily_summary(spread_id, trading_date DESC);

-- ===================================
-- Insert Initial Master Data
-- ===================================

-- Insert metal configurations
INSERT INTO lme_config.LME_M_metals (metal_code, metal_name, bloomberg_base) VALUES
    ('CU', 'Copper', 'LMCADS'),
    ('AL', 'Aluminum', 'LMAHDS'),
    ('ZN', 'Zinc', 'LMZSDS'),
    ('PB', 'Lead', 'LMPBDS'),
    ('NI', 'Nickel', 'LMNIDS'),
    ('SN', 'Tin', 'LMSNDS');

-- Insert default collection configurations (example for Copper)
INSERT INTO lme_config.LME_M_collection_config (metal_id, collection_type, interval_minutes) VALUES
    (1, 'REALTIME', 5),    -- Active spreads every 5 minutes
    (1, 'REGULAR', 30),    -- All spreads every 30 minutes
    (1, 'DAILY', 1440);    -- New spread discovery daily

-- Verify table creation
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    t.create_date
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name IN ('lme_market', 'lme_config')
ORDER BY s.name, t.name;

PRINT '';
PRINT 'LME tables created successfully in JCL database';
PRINT 'Next step: Run 03_create_procedures_in_JCL.sql';