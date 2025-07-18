-- Add actual spread type fields to LME_M_spreads table
-- Version: 1.1 - Fixed for Azure SQL
-- Date: 2025-07-18
-- Purpose: Add fields to correctly classify spreads that use date notation instead of standard codes

USE JCL;
GO

-- Set required options for Azure SQL
SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO

-- Check if columns already exist before adding
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('lme_market.LME_M_spreads') AND name = 'actual_spread_type')
BEGIN
    ALTER TABLE lme_market.LME_M_spreads ADD actual_spread_type NVARCHAR(20);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('lme_market.LME_M_spreads') AND name = 'actual_leg1_type')
BEGIN
    ALTER TABLE lme_market.LME_M_spreads ADD actual_leg1_type NVARCHAR(20);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('lme_market.LME_M_spreads') AND name = 'actual_leg2_type')
BEGIN
    ALTER TABLE lme_market.LME_M_spreads ADD actual_leg2_type NVARCHAR(20);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('lme_market.LME_M_spreads') AND name = 'is_verified')
BEGIN
    ALTER TABLE lme_market.LME_M_spreads ADD is_verified BIT DEFAULT 0;
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('lme_market.LME_M_spreads') AND name = 'classification_notes')
BEGIN
    ALTER TABLE lme_market.LME_M_spreads ADD classification_notes NVARCHAR(255);
END
GO

-- Create index if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('lme_market.LME_M_spreads') AND name = 'IX_LME_M_spreads_actual_type')
BEGIN
    CREATE INDEX IX_LME_M_spreads_actual_type 
    ON lme_market.LME_M_spreads(actual_spread_type) 
    WHERE actual_spread_type IS NOT NULL;
END
GO

PRINT 'Successfully added/verified actual spread type fields';
GO

-- Create or alter the comparison view
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

PRINT 'Created/updated spread classification comparison view';
GO