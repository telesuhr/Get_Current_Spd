-- Add actual spread type fields to LME_M_spreads table
-- Version: 1.0
-- Date: 2025-07-18
-- Purpose: Add fields to correctly classify spreads that use date notation instead of standard codes

USE JCL;
GO

-- Add new columns for actual spread classification
ALTER TABLE lme_market.LME_M_spreads ADD
    actual_spread_type NVARCHAR(20),      -- Actual spread type (e.g., Cash-3W, 3M-3W)
    actual_leg1_type NVARCHAR(20),        -- Actual leg1 type (Cash, 3M, 3W, Odd)
    actual_leg2_type NVARCHAR(20),        -- Actual leg2 type
    is_verified BIT DEFAULT 0,            -- Manual verification flag
    classification_notes NVARCHAR(255);   -- Notes about classification logic
GO

-- Create index on actual_spread_type for performance
CREATE INDEX IX_LME_M_spreads_actual_type 
ON lme_market.LME_M_spreads(actual_spread_type) 
WHERE actual_spread_type IS NOT NULL;
GO

-- Update column descriptions
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Actual spread type based on date analysis (Cash-3W, 3M-3W, etc.)', 
    @level0type = N'SCHEMA', @level0name = N'lme_market',
    @level1type = N'TABLE',  @level1name = N'LME_M_spreads',
    @level2type = N'COLUMN', @level2name = N'actual_spread_type';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Actual type of first leg (Cash, 3M, 3W=Third Wednesday, Odd)', 
    @level0type = N'SCHEMA', @level0name = N'lme_market',
    @level1type = N'TABLE',  @level1name = N'LME_M_spreads',
    @level2type = N'COLUMN', @level2name = N'actual_leg1_type';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Actual type of second leg (Cash, 3M, 3W=Third Wednesday, Odd)', 
    @level0type = N'SCHEMA', @level0name = N'lme_market',
    @level1type = N'TABLE',  @level1name = N'LME_M_spreads',
    @level2type = N'COLUMN', @level2name = N'actual_leg2_type';

PRINT 'Added actual spread type fields to LME_M_spreads table';
GO

-- Create a view to show spread classification comparison
CREATE OR ALTER VIEW lme_market.V_spread_classification_comparison
AS
SELECT 
    m.metal_code,
    s.ticker,
    s.spread_type as original_type,
    s.actual_spread_type,
    s.prompt_date1,
    s.prompt_date2,
    s.leg1_description,
    s.leg2_description,
    s.actual_leg1_type,
    s.actual_leg2_type,
    s.is_verified,
    CASE 
        WHEN s.spread_type != s.actual_spread_type THEN 'Reclassified'
        WHEN s.actual_spread_type IS NULL THEN 'Not Analyzed'
        ELSE 'Matched'
    END as classification_status
FROM lme_market.LME_M_spreads s
JOIN lme_config.LME_M_metals m ON s.metal_id = m.metal_id
WHERE s.is_active = 1;
GO

PRINT 'Created spread classification comparison view';
GO