@echo off
REM Weekly LME Spread Maintenance
REM Run this script weekly (e.g., every Monday at 15:00 JST)

cd /d C:\Users\09848\Git\GetAllSpd

echo ========================================
echo LME Weekly Maintenance
echo Started at: %date% %time%
echo ========================================

echo.
echo [1/3] Updating prompt dates for new spreads...
python scripts\sql_collector\update_prompt_dates.py

if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Prompt date update failed
    goto :skip_classify
)

echo.
echo [2/3] Classifying actual spread types...
python scripts\sql_collector\classify_actual_spreads.py

if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Spread classification failed
)

:skip_classify
echo.
echo [3/3] Running daily data collection...
python scripts\sql_collector\quick_collect_copper.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [SUCCESS] Weekly maintenance completed successfully
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