@echo off
echo === Checking Aerospike Server 1 (Producer) ===
echo Namespaces:
docker exec -it aerospike asinfo -v "namespaces"
echo.
echo Objects in each namespace:
set "producer_total=0"
for /f "tokens=*" %%a in ('docker exec -it aerospike asinfo -v "namespaces"') do (
    for %%b in (%%a) do (
        echo === Namespace: %%b ===
        for /f "tokens=3 delims=;" %%c in ('docker exec -it aerospike asinfo -v "namespace/%%b" ^| findstr "objects="') do (
            echo Objects: %%c
            set /a "producer_total+=%%c"
        )
        echo.
    )
)

echo === Checking Aerospike Server 2 (Consumer) ===
echo Namespaces:
docker exec -it aerospike2 asinfo -v "namespaces"
echo.
echo Objects in each namespace:
set "consumer_total=0"
for /f "tokens=*" %%a in ('docker exec -it aerospike2 asinfo -v "namespaces"') do (
    for %%b in (%%a) do (
        echo === Namespace: %%b ===
        for /f "tokens=3 delims=;" %%c in ('docker exec -it aerospike2 asinfo -v "namespace/%%b" ^| findstr "objects="') do (
            echo Objects: %%c
            set /a "consumer_total+=%%c"
        )
        echo.
    )
)

echo === Record Count Comparison ===
echo Producer Total Records: %producer_total%
echo Consumer Total Records: %consumer_total%
if %producer_total% EQU %consumer_total% (
    echo Status: MATCHED
) else (
    echo Status: MISMATCHED
    set /a "diff=%producer_total%-%consumer_total%"
    echo Difference: %diff% records
)


pause 