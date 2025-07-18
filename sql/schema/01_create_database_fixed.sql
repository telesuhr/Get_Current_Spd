-- LME Metal Spreads Database Schema (Fixed for Azure SQL Database compatibility)
-- Version: 2.1
-- Date: 2025-07-18
-- Supports multiple metals: Copper, Aluminum, Zinc, Lead, Nickel, Tin

-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'LMEMetalSpreads')
BEGIN
    CREATE DATABASE LMEMetalSpreads;
END
GO

-- Note: For Azure SQL Database or restricted environments, 
-- USE statement and ALTER DATABASE commands may need to be run separately
-- or from within the target database connection.

PRINT 'Database created successfully';
PRINT 'Please connect to LMEMetalSpreads database to continue with table creation';
GO