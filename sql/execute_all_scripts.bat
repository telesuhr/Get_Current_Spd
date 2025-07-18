@echo off
REM Execute all SQL scripts to create the database
REM Version: 2.0
REM Date: 2025-07-18

echo ========================================
echo LME Metal Spreads Database Setup
echo ========================================

REM Set SQL Server connection parameters
set SERVER=localhost
set DATABASE=master

echo.
echo Step 1: Creating database and schemas...
sqlcmd -S %SERVER% -d %DATABASE% -i schema\01_create_database.sql
if %ERRORLEVEL% neq 0 goto :error

echo.
echo Step 2: Creating tables with new naming convention...
sqlcmd -S %SERVER% -d LMEMetalSpreads -i schema\02_create_tables_v2.sql
if %ERRORLEVEL% neq 0 goto :error

echo.
echo Step 3: Creating stored procedures...
sqlcmd -S %SERVER% -d LMEMetalSpreads -i procedures\01_insert_procedures_v2.sql
if %ERRORLEVEL% neq 0 goto :error

echo.
echo Step 4: Creating views...
sqlcmd -S %SERVER% -d LMEMetalSpreads -i views\01_market_views_v2.sql
if %ERRORLEVEL% neq 0 goto :error

echo.
echo ========================================
echo Database setup completed successfully!
echo ========================================
echo.
echo You can now run the test script:
echo python ..\scripts\sql_collector\test_sql_system.py
echo.
pause
exit /b 0

:error
echo.
echo ========================================
echo ERROR: Database setup failed!
echo ========================================
pause
exit /b 1