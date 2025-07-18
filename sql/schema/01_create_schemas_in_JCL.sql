-- LME Metal Spreads Schemas for JCL Database
-- Version: 3.0
-- Date: 2025-07-18
-- Creates LME-specific schemas within existing JCL database

-- Connect to JCL database before running this script

-- Create LME-specific schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'lme_market')
BEGIN
    EXEC('CREATE SCHEMA lme_market AUTHORIZATION dbo');
    PRINT 'Created schema: lme_market';
END
ELSE
BEGIN
    PRINT 'Schema lme_market already exists';
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'lme_config')
BEGIN
    EXEC('CREATE SCHEMA lme_config AUTHORIZATION dbo');
    PRINT 'Created schema: lme_config';
END
ELSE
BEGIN
    PRINT 'Schema lme_config already exists';
END
GO

-- Verify schemas were created
SELECT 
    s.schema_id,
    s.name as schema_name,
    p.name as principal_name
FROM sys.schemas s
LEFT JOIN sys.database_principals p ON s.principal_id = p.principal_id
WHERE s.name IN ('lme_market', 'lme_config')
ORDER BY s.name;

PRINT '';
PRINT 'LME schemas created successfully in JCL database';
PRINT 'Next step: Run 02_create_tables_in_JCL.sql';