-- Create views using actual spread types
-- Version: 1.0
-- Date: 2025-07-18
-- Purpose: Create views that use actual_spread_type for accurate reporting

USE JCL;
GO

-- 1. Active spreads with actual classification
CREATE OR ALTER VIEW lme_market.V_active_spreads_actual
AS
SELECT 
    m.metal_code,
    s.ticker,
    s.spread_type as original_type,
    COALESCE(s.actual_spread_type, s.spread_type) as spread_type,  -- Use actual if available
    s.actual_leg1_type,
    s.actual_leg2_type,
    s.prompt_date1,
    s.prompt_date2,
    s.leg1_description,
    s.leg2_description,
    md.bid_price,
    md.ask_price,
    md.last_price,
    md.volume,
    md.last_update_dt,
    s.classification_notes
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
LEFT JOIN lme_market.V_latest_market_data md ON s.spread_id = md.spread_id
WHERE s.is_active = 1;
GO

-- 2. Spread type summary using actual types
CREATE OR ALTER VIEW lme_market.V_spread_type_summary_actual
AS
SELECT 
    m.metal_code,
    COALESCE(s.actual_spread_type, s.spread_type) as spread_type,
    COUNT(DISTINCT s.spread_id) as spread_count,
    COUNT(DISTINCT CASE WHEN md.last_update_dt >= CAST(GETDATE() AS DATE) THEN s.spread_id END) as active_today,
    AVG(md.volume) as avg_volume,
    MAX(md.last_update_dt) as last_activity
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
LEFT JOIN lme_market.V_latest_market_data md ON s.spread_id = md.spread_id
WHERE s.is_active = 1
GROUP BY m.metal_code, COALESCE(s.actual_spread_type, s.spread_type);
GO

-- 3. Cash-3W spreads specifically
CREATE OR ALTER VIEW lme_market.V_cash_3w_spreads
AS
SELECT 
    m.metal_code,
    s.ticker,
    s.prompt_date1 as cash_date,
    s.prompt_date2 as third_wed_date,
    DATENAME(MONTH, s.prompt_date2) + ' ' + CAST(YEAR(s.prompt_date2) as VARCHAR(4)) as contract_month,
    md.bid_price,
    md.ask_price,
    md.last_price,
    md.volume,
    md.last_update_dt
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
LEFT JOIN lme_market.V_latest_market_data md ON s.spread_id = md.spread_id
WHERE s.actual_spread_type = 'Cash-3W'
  AND s.is_active = 1
ORDER BY s.prompt_date2;
GO

-- 4. Reclassified spreads report
CREATE OR ALTER VIEW lme_market.V_reclassified_spreads_report
AS
SELECT 
    m.metal_code,
    s.ticker,
    s.spread_type as ticker_format_type,
    s.actual_spread_type as actual_type,
    s.prompt_date1,
    s.prompt_date2,
    md.volume,
    md.last_update_dt,
    s.classification_notes
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
LEFT JOIN lme_market.V_latest_market_data md ON s.spread_id = md.spread_id
WHERE s.spread_type != s.actual_spread_type
  AND s.actual_spread_type IS NOT NULL
  AND s.is_active = 1;
GO

PRINT 'Created views using actual spread types';
GO