-- LME Metal Spreads Database Schema
-- Version: 1.0
-- Date: 2025-07-18
-- Supports multiple metals: Copper, Aluminum, Zinc, Lead, Nickel, Tin

-- Create database if not exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'LMEMetalSpreads')
BEGIN
    CREATE DATABASE LMEMetalSpreads;
END
GO

USE LMEMetalSpreads;
GO

-- Enable snapshot isolation for better concurrent read performance
ALTER DATABASE LMEMetalSpreads SET ALLOW_SNAPSHOT_ISOLATION ON;
ALTER DATABASE LMEMetalSpreads SET READ_COMMITTED_SNAPSHOT ON;
GO

-- Create schema for better organization
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

PRINT 'Database and schemas created successfully';