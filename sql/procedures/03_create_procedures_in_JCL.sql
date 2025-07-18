-- Stored Procedures for JCL Database LME System
-- Version: 3.0
-- Date: 2025-07-18

-- Ensure you are connected to JCL database before running

-- Drop existing procedures if they exist
IF OBJECT_ID('lme_market.sp_UpsertSpread', 'P') IS NOT NULL DROP PROCEDURE lme_market.sp_UpsertSpread;
IF OBJECT_ID('lme_market.sp_InsertTickData', 'P') IS NOT NULL DROP PROCEDURE lme_market.sp_InsertTickData;
IF OBJECT_ID('lme_market.sp_BulkInsertTickData', 'P') IS NOT NULL DROP PROCEDURE lme_market.sp_BulkInsertTickData;
IF OBJECT_ID('lme_market.sp_CalculateDailySummary', 'P') IS NOT NULL DROP PROCEDURE lme_market.sp_CalculateDailySummary;
IF OBJECT_ID('lme_market.sp_GetActiveSpreads', 'P') IS NOT NULL DROP PROCEDURE lme_market.sp_GetActiveSpreads;
GO

-- Drop table type if exists
IF EXISTS (SELECT * FROM sys.types WHERE name = 'LME_TickDataType' AND schema_id = SCHEMA_ID('lme_market'))
    DROP TYPE lme_market.LME_TickDataType;
GO

-- Create table type for bulk insert
CREATE TYPE lme_market.LME_TickDataType AS TABLE
(
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
    rt_spread_bp DECIMAL(10,2),
    contract_value DECIMAL(18,2)
);
GO

-- Procedure to insert or update spread definition
CREATE PROCEDURE lme_market.sp_UpsertSpread
    @metal_code NVARCHAR(10),
    @ticker NVARCHAR(50),
    @spread_type NVARCHAR(20),
    @description NVARCHAR(255) = NULL,
    @prompt_date1 DATE = NULL,
    @prompt_date2 DATE = NULL,
    @leg1_description NVARCHAR(100) = NULL,
    @leg2_description NVARCHAR(100) = NULL,
    @spread_id INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @metal_id INT;
    
    -- Get metal_id
    SELECT @metal_id = metal_id 
    FROM lme_config.LME_M_metals 
    WHERE metal_code = @metal_code;
    
    IF @metal_id IS NULL
    BEGIN
        RAISERROR('Invalid metal code: %s', 16, 1, @metal_code);
        RETURN;
    END
    
    -- Try to find existing spread
    SELECT @spread_id = spread_id
    FROM lme_market.LME_M_spreads
    WHERE metal_id = @metal_id AND ticker = @ticker;
    
    IF @spread_id IS NULL
    BEGIN
        -- Insert new spread
        INSERT INTO lme_market.LME_M_spreads (
            metal_id, ticker, spread_type, description,
            prompt_date1, prompt_date2, leg1_description, leg2_description
        )
        VALUES (
            @metal_id, @ticker, @spread_type, @description,
            @prompt_date1, @prompt_date2, @leg1_description, @leg2_description
        );
        
        SET @spread_id = SCOPE_IDENTITY();
    END
    ELSE
    BEGIN
        -- Update existing spread
        UPDATE lme_market.LME_M_spreads
        SET 
            spread_type = @spread_type,
            description = COALESCE(@description, description),
            prompt_date1 = COALESCE(@prompt_date1, prompt_date1),
            prompt_date2 = COALESCE(@prompt_date2, prompt_date2),
            leg1_description = COALESCE(@leg1_description, leg1_description),
            leg2_description = COALESCE(@leg2_description, leg2_description),
            last_seen_date = CAST(GETDATE() AS DATE),
            updated_at = GETDATE()
        WHERE spread_id = @spread_id;
    END
END
GO

-- Procedure to insert tick data with duplicate prevention
CREATE PROCEDURE lme_market.sp_InsertTickData
    @spread_id INT,
    @timestamp DATETIME2(3),
    @bid DECIMAL(12,4) = NULL,
    @ask DECIMAL(12,4) = NULL,
    @last_price DECIMAL(12,4) = NULL,
    @bid_size INT = NULL,
    @ask_size INT = NULL,
    @volume BIGINT = NULL,
    @todays_volume BIGINT = NULL,
    @open_interest INT = NULL,
    @last_update_dt DATE = NULL,
    @trading_dt DATE = NULL,
    @rt_spread_bp DECIMAL(10,2) = NULL,
    @contract_value DECIMAL(18,2) = NULL,
    @duplicate_check BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Duplicate check (optional)
    IF @duplicate_check = 1
    BEGIN
        IF EXISTS (
            SELECT 1 
            FROM lme_market.LME_T_tick_data 
            WHERE spread_id = @spread_id 
            AND timestamp = @timestamp
            AND ISNULL(bid, -999999) = ISNULL(@bid, -999999)
            AND ISNULL(ask, -999999) = ISNULL(@ask, -999999)
            AND ISNULL(last_price, -999999) = ISNULL(@last_price, -999999)
        )
        BEGIN
            RETURN; -- Skip duplicate
        END
    END
    
    -- Insert tick data
    INSERT INTO lme_market.LME_T_tick_data (
        spread_id, timestamp, bid, ask, last_price,
        bid_size, ask_size, volume, todays_volume,
        open_interest, last_update_dt, trading_dt,
        rt_spread_bp, contract_value
    )
    VALUES (
        @spread_id, @timestamp, @bid, @ask, @last_price,
        @bid_size, @ask_size, @volume, @todays_volume,
        @open_interest, @last_update_dt, @trading_dt,
        @rt_spread_bp, @contract_value
    );
END
GO

-- Bulk insert procedure for performance
CREATE PROCEDURE lme_market.sp_BulkInsertTickData
    @TickData lme_market.LME_TickDataType READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert only non-duplicate records
    INSERT INTO lme_market.LME_T_tick_data (
        spread_id, timestamp, bid, ask, last_price,
        bid_size, ask_size, volume, todays_volume,
        open_interest, last_update_dt, trading_dt,
        rt_spread_bp, contract_value
    )
    SELECT 
        td.spread_id, td.timestamp, td.bid, td.ask, td.last_price,
        td.bid_size, td.ask_size, td.volume, td.todays_volume,
        td.open_interest, td.last_update_dt, td.trading_dt,
        td.rt_spread_bp, td.contract_value
    FROM @TickData td
    WHERE NOT EXISTS (
        SELECT 1 
        FROM lme_market.LME_T_tick_data existing
        WHERE existing.spread_id = td.spread_id 
        AND existing.timestamp = td.timestamp
        AND ISNULL(existing.bid, -999999) = ISNULL(td.bid, -999999)
        AND ISNULL(existing.ask, -999999) = ISNULL(td.ask, -999999)
        AND ISNULL(existing.last_price, -999999) = ISNULL(td.last_price, -999999)
    );
    
    SELECT @@ROWCOUNT AS RecordsInserted;
END
GO

-- Procedure to calculate and insert daily summary
CREATE PROCEDURE lme_market.sp_CalculateDailySummary
    @trading_date DATE = NULL,
    @metal_id INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Use today if no date specified
    IF @trading_date IS NULL
        SET @trading_date = CAST(GETDATE() AS DATE);
    
    -- Calculate summaries
    WITH DaySummary AS (
        SELECT 
            t.spread_id,
            @trading_date as trading_date,
            MIN(t.last_price) as low_price,
            MAX(t.last_price) as high_price,
            MAX(t.todays_volume) as total_volume,
            COUNT(DISTINCT t.timestamp) as trade_count,
            AVG(CASE WHEN t.bid IS NOT NULL AND t.ask IS NOT NULL 
                     THEN t.ask - t.bid ELSE NULL END) as avg_bid_ask_spread,
            MAX(CASE WHEN t.bid IS NOT NULL AND t.ask IS NOT NULL 
                     THEN t.ask - t.bid ELSE NULL END) as max_bid_ask_spread,
            COUNT(DISTINCT DATEPART(HOUR, t.timestamp) * 60 + DATEPART(MINUTE, t.timestamp)) as time_with_quotes
        FROM lme_market.LME_T_tick_data t
        JOIN lme_market.LME_M_spreads s ON t.spread_id = s.spread_id
        WHERE CAST(t.timestamp AS DATE) = @trading_date
        AND (@metal_id IS NULL OR s.metal_id = @metal_id)
        GROUP BY t.spread_id
    ),
    OpenClose AS (
        SELECT DISTINCT
            spread_id,
            FIRST_VALUE(last_price) OVER (PARTITION BY spread_id ORDER BY timestamp) as open_price,
            LAST_VALUE(last_price) OVER (PARTITION BY spread_id ORDER BY timestamp 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as close_price
        FROM lme_market.LME_T_tick_data
        WHERE CAST(timestamp AS DATE) = @trading_date
        AND last_price IS NOT NULL
    ),
    VWAP AS (
        SELECT 
            spread_id,
            SUM(last_price * todays_volume) / NULLIF(SUM(todays_volume), 0) as vwap
        FROM lme_market.LME_T_tick_data
        WHERE CAST(timestamp AS DATE) = @trading_date
        AND last_price IS NOT NULL AND todays_volume > 0
        GROUP BY spread_id
    )
    -- Insert or update daily summaries
    MERGE lme_market.LME_T_daily_summary AS target
    USING (
        SELECT 
            ds.spread_id,
            ds.trading_date,
            oc.open_price,
            ds.high_price,
            ds.low_price,
            oc.close_price,
            v.vwap,
            ds.total_volume,
            ds.trade_count,
            ds.avg_bid_ask_spread,
            ds.max_bid_ask_spread,
            ds.time_with_quotes
        FROM DaySummary ds
        LEFT JOIN OpenClose oc ON ds.spread_id = oc.spread_id
        LEFT JOIN VWAP v ON ds.spread_id = v.spread_id
    ) AS source
    ON target.spread_id = source.spread_id AND target.trading_date = source.trading_date
    WHEN MATCHED THEN
        UPDATE SET
            open_price = source.open_price,
            high_price = source.high_price,
            low_price = source.low_price,
            close_price = source.close_price,
            vwap = source.vwap,
            total_volume = source.total_volume,
            trade_count = source.trade_count,
            avg_bid_ask_spread = source.avg_bid_ask_spread,
            max_bid_ask_spread = source.max_bid_ask_spread,
            time_with_quotes = source.time_with_quotes
    WHEN NOT MATCHED THEN
        INSERT (spread_id, trading_date, open_price, high_price, low_price, 
                close_price, vwap, total_volume, trade_count, avg_bid_ask_spread, 
                max_bid_ask_spread, time_with_quotes)
        VALUES (source.spread_id, source.trading_date, source.open_price, 
                source.high_price, source.low_price, source.close_price, 
                source.vwap, source.total_volume, source.trade_count, 
                source.avg_bid_ask_spread, source.max_bid_ask_spread, 
                source.time_with_quotes);
    
    SELECT @@ROWCOUNT AS SummariesProcessed;
END
GO

-- Procedure to get active spreads
CREATE PROCEDURE lme_market.sp_GetActiveSpreads
    @metal_code NVARCHAR(10),
    @hours INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT DISTINCT
        s.spread_id,
        s.ticker,
        s.spread_type,
        s.description,
        s.prompt_date1,
        s.prompt_date2
    FROM lme_market.LME_M_spreads s
    JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
    JOIN lme_market.LME_T_tick_data t ON s.spread_id = t.spread_id
    WHERE m.metal_code = @metal_code
    AND t.timestamp > DATEADD(HOUR, -@hours, GETDATE())
    AND (t.bid IS NOT NULL OR t.ask IS NOT NULL)
    AND s.is_active = 1
    ORDER BY s.ticker;
END
GO

-- Verify procedure creation
SELECT 
    s.name AS SchemaName,
    p.name AS ProcedureName,
    p.create_date
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name = 'lme_market'
ORDER BY p.name;

PRINT '';
PRINT 'LME procedures created successfully in JCL database';
PRINT 'Next step: Run 04_create_views_in_JCL.sql';