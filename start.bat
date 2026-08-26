@echo off
echo =========================================
echo   VN Stock Monitor – Starting...
echo =========================================

cd /d "%~dp0backend"

:: Check Python launcher
py --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Install Python 3.10+ first.
    pause
    exit /b 1
)

:: Install dependencies if needed
py -m pip show fastapi >nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing dependencies...
    py -m pip install -r requirements.txt
)

echo.
echo [INFO] Starting backend on http://localhost:8000
echo [INFO] Open frontend\index.html in your browser
echo [INFO] Press Ctrl+C to stop
echo.

py main.py
pause
