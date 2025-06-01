@echo off
setlocal enabledelayedexpansion

echo Cleaning up Aerospike data files...
del /F /Q "docker\aerospike_data\consumer_033.dat"
del /F /Q "docker\aerospike_data\consumer_096.dat"

echo Waiting for 2 seconds...
timeout /t 2 /nobreak > nul

echo Restarting Aerospike container...
docker restart aerospike2

echo Done! 