@echo off
REM Daily LME Copper Spread Data Collection
REM Run this script daily at 16:30 JST and 01:30 JST

cd /d C:\Users\09848\Git\GetAllSpd

echo ========================================
echo LME Daily Data Collection
echo Started at: %date% %time%
echo ========================================

echo.
echo Running copper spread data collection...
python scripts\sql_collector\quick_collect_copper.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [SUCCESS] Data collection completed successfully
) else (
    echo.
    echo [ERROR] Data collection failed with error code: %ERRORLEVEL%
    echo Please check logs\lme_collector.log for details
)

echo.
echo ========================================
echo Completed at: %date% %time%
echo ========================================

pause