-- Check reclassified spreads
SELECT TOP 20 
    ticker,
    spread_type as original_type,
    actual_spread_type,
    actual_leg1_type + '-' + actual_leg2_type as leg_types,
    CONVERT(VARCHAR(10), prompt_date1, 120) as date1,
    CONVERT(VARCHAR(10), prompt_date2, 120) as date2
FROM lme_market.LME_M_spreads 
WHERE actual_spread_type != spread_type 
    AND ticker LIKE '%250722%'
ORDER BY ticker;