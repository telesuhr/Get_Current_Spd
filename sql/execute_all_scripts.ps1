# PowerShell script to execute all SQL scripts
# Version: 2.0
# Date: 2025-07-18

Write-Host "========================================"
Write-Host "LME Metal Spreads Database Setup"
Write-Host "========================================"

# SQL Server connection parameters
$Server = "localhost"
$Database = "master"

# Function to execute SQL script
function Execute-SqlScript {
    param(
        [string]$ScriptPath,
        [string]$DatabaseName = $Database,
        [string]$Description
    )
    
    Write-Host "`n$Description..."
    
    try {
        Invoke-Sqlcmd -ServerInstance $Server -Database $DatabaseName -InputFile $ScriptPath -ErrorAction Stop
        Write-Host "✓ Success" -ForegroundColor Green
    }
    catch {
        Write-Host "✗ Failed: $_" -ForegroundColor Red
        throw
    }
}

try {
    # Step 1: Create database
    Execute-SqlScript -ScriptPath "schema\01_create_database.sql" -Description "Step 1: Creating database and schemas"
    
    # Step 2: Create tables
    Execute-SqlScript -ScriptPath "schema\02_create_tables_v2.sql" -DatabaseName "LMEMetalSpreads" -Description "Step 2: Creating tables with new naming convention"
    
    # Step 3: Create procedures
    Execute-SqlScript -ScriptPath "procedures\01_insert_procedures_v2.sql" -DatabaseName "LMEMetalSpreads" -Description "Step 3: Creating stored procedures"
    
    # Step 4: Create views
    Execute-SqlScript -ScriptPath "views\01_market_views_v2.sql" -DatabaseName "LMEMetalSpreads" -Description "Step 4: Creating views"
    
    Write-Host "`n========================================"
    Write-Host "Database setup completed successfully!" -ForegroundColor Green
    Write-Host "========================================"
    Write-Host "`nYou can now run the test script:"
    Write-Host "python ..\scripts\sql_collector\test_sql_system.py" -ForegroundColor Yellow
}
catch {
    Write-Host "`n========================================"
    Write-Host "ERROR: Database setup failed!" -ForegroundColor Red
    Write-Host "========================================"
    Write-Host $_.Exception.Message -ForegroundColor Red
}

Write-Host "`nPress any key to continue..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")