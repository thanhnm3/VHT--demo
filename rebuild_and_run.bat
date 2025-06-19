@echo off
echo ========================================
echo Rebuilding and Running VHT Pipeline
echo ========================================
echo.

echo Cleaning and rebuilding...
cd docker

echo Stopping existing containers...
docker-compose -f docker-compose-pipeline.yml down 2>nul

echo Building new image...
docker-compose -f docker-compose-pipeline.yml build --no-cache

if %ERRORLEVEL% NEQ 0 (
    echo Error: Failed to build Docker image
    pause
    exit /b 1
)

echo.
echo Starting pipeline...
docker-compose -f docker-compose-pipeline.yml up

echo.
echo Pipeline completed.
pause 