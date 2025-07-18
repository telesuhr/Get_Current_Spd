-- Configure Database Settings
-- Version: 2.1
-- Date: 2025-07-18
-- Run this AFTER connecting to LMEMetalSpreads database

-- Enable snapshot isolation for better concurrent read performance
ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON;
GO

ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON;
GO

-- Create schemas for better organization
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'market')
BEGIN
    EXEC('CREATE SCHEMA market');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'config')
BEGIN
    EXEC('CREATE SCHEMA config');
END
GO

PRINT 'Database configuration completed successfully';