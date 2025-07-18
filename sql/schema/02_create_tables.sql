-- LME Metal Spreads Tables
-- Version: 1.0
-- Date: 2025-07-18

USE LMEMetalSpreads;
GO

-- Drop tables if they exist (in correct order due to foreign keys)
IF OBJECT_ID('market.tick_data', 'U') IS NOT NULL DROP TABLE market.tick_data;
IF OBJECT_ID('market.daily_summary', 'U') IS NOT NULL DROP TABLE market.daily_summary;
IF OBJECT_ID('market.spreads', 'U') IS NOT NULL DROP TABLE market.spreads;
IF OBJECT_ID('config.metals', 'U') IS NOT NULL DROP TABLE config.metals;
IF OBJECT_ID('config.collection_config', 'U') IS NOT NULL DROP TABLE config.collection_config;
GO

-- Configuration Tables

-- Metal master table
CREATE TABLE config.metals (
    metal_id INT IDENTITY(1,1) PRIMARY KEY,
    metal_code NVARCHAR(10) NOT NULL UNIQUE,  -- e.g., 'CU', 'AL', 'ZN'
    metal_name NVARCHAR(50) NOT NULL,         -- e.g., 'Copper', 'Aluminum'
    bloomberg_base NVARCHAR(20) NOT NULL,     -- e.g., 'LMCADS' for copper
    exchange_code NVARCHAR(10) DEFAULT 'LME',
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- Collection configuration
CREATE TABLE config.collection_config (
    config_id INT IDENTITY(1,1) PRIMARY KEY,
    metal_id INT NOT NULL,
    collection_type NVARCHAR(20) NOT NULL,    -- 'REALTIME', 'REGULAR', 'DAILY'
    interval_minutes INT NOT NULL,
    is_active BIT DEFAULT 1,
    last_run DATETIME2,
    next_run DATETIME2,
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (metal_id) REFERENCES config.metals(metal_id)
);

-- Market Data Tables

-- Spread definitions
CREATE TABLE market.spreads (
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
    FOREIGN KEY (metal_id) REFERENCES config.metals(metal_id)
);

-- Tick data (main transaction table)
CREATE TABLE market.tick_data (
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
    FOREIGN KEY (spread_id) REFERENCES market.spreads(spread_id)
);

-- Daily summary table
CREATE TABLE market.daily_summary (
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
    FOREIGN KEY (spread_id) REFERENCES market.spreads(spread_id)
);

-- Create indexes for performance
CREATE INDEX IX_spreads_metal_type ON market.spreads(metal_id, spread_type) WHERE is_active = 1;
CREATE INDEX IX_spreads_prompt_dates ON market.spreads(prompt_date1, prompt_date2);

CREATE INDEX IX_tick_timestamp ON market.tick_data(timestamp) INCLUDE (spread_id);
CREATE INDEX IX_tick_spread_timestamp ON market.tick_data(spread_id, timestamp DESC);
CREATE INDEX IX_tick_todays_volume ON market.tick_data(todays_volume) WHERE todays_volume > 0;
CREATE INDEX IX_tick_trading_dt ON market.tick_data(trading_dt, spread_id);

CREATE INDEX IX_daily_date ON market.daily_summary(trading_date DESC);
CREATE INDEX IX_daily_spread_date ON market.daily_summary(spread_id, trading_date DESC);

-- Insert initial metal configurations
INSERT INTO config.metals (metal_code, metal_name, bloomberg_base) VALUES
    ('CU', 'Copper', 'LMCADS'),
    ('AL', 'Aluminum', 'LMAHDS'),
    ('ZN', 'Zinc', 'LMZSDS'),
    ('PB', 'Lead', 'LMPBDS'),
    ('NI', 'Nickel', 'LMNIDS'),
    ('SN', 'Tin', 'LMSNDS');

-- Insert default collection configurations (example for Copper)
INSERT INTO config.collection_config (metal_id, collection_type, interval_minutes) VALUES
    (1, 'REALTIME', 5),    -- Active spreads every 5 minutes
    (1, 'REGULAR', 30),    -- All spreads every 30 minutes
    (1, 'DAILY', 1440);    -- New spread discovery daily

PRINT 'Tables created successfully';