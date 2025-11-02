@echo off
echo Starting Real-Time Web Server Log Analysis Demo
echo ================================================

echo.
echo Checking if Docker is running...
docker --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not installed or not running
    echo Please install Docker Desktop and start it
    pause
    exit /b 1
)

echo.
echo Starting infrastructure services...
docker-compose up -d

echo.
echo Waiting for services to start (30 seconds)...
timeout /t 30 /nobreak >nul

echo.
echo Installing Python dependencies...
pip install -r requirements.txt

echo.
echo Starting the interactive demo...
python scripts/run_demo.py

echo.
echo Demo completed. Press any key to stop services...
pause >nul

echo.
echo Stopping services...
docker-compose down

echo.
echo All done! Press any key to exit...
pause >nul