-- View spreads with actual classification
-- Shows how Bloomberg ticker notation maps to actual spread types

-- 1. Cash-3W spreads that appear as Odd-Odd in tickers
SELECT 
    ticker,
    spread_type as ticker_shows,
    actual_spread_type as actually_is,
    CONVERT(VARCHAR(10), prompt_date1, 120) as date1,
    CONVERT(VARCHAR(10), prompt_date2, 120) as date2,
    last_price,
    todays_volume as volume,
    last_update_dt
FROM lme_market.V_active_spreads_actual
WHERE actual_spread_type = 'Cash-3W'
  AND metal_code = 'CU'
  AND last_update_dt >= CAST(GETDATE() AS DATE)
ORDER BY prompt_date2;

-- 2. Summary of actual spread types with activity
SELECT 
    spread_type,
    COUNT(*) as total_spreads,
    SUM(CASE WHEN last_update_dt >= CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END) as active_today,
    MAX(last_update_dt) as last_activity
FROM lme_market.V_active_spreads_actual
WHERE metal_code = 'CU'
GROUP BY spread_type
ORDER BY total_spreads DESC;

-- 3. View reclassified spreads
SELECT TOP 20
    ticker,
    ticker_format_type as shows_as,
    actual_type as actually_is,
    CONVERT(VARCHAR(10), prompt_date1, 120) as date1,
    CONVERT(VARCHAR(10), prompt_date2, 120) as date2,
    volume,
    last_update_dt
FROM lme_market.V_reclassified_spreads_report
WHERE metal_code = 'CU'
ORDER BY last_update_dt DESC;