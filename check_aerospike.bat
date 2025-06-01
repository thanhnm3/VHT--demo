@echo off
echo === Checking Aerospike Server 1 ===
echo Namespaces:
docker exec -it aerospike asinfo -v "namespaces"
echo.
echo Objects in each namespace:
for /f "tokens=*" %%a in ('docker exec -it aerospike asinfo -v "namespaces"') do (
    for %%b in (%%a) do (
        echo === Namespace: %%b ===
        for /f "tokens=3 delims=;" %%c in ('docker exec -it aerospike asinfo -v "namespace/%%b" ^| findstr "objects="') do (
            echo Objects: %%c
        )
        echo.
    )
)

echo === Checking Aerospike Server 2 ===
echo Namespaces:
docker exec -it aerospike2 asinfo -v "namespaces"
echo.
echo Objects in each namespace:
for /f "tokens=*" %%a in ('docker exec -it aerospike2 asinfo -v "namespaces"') do (
    for %%b in (%%a) do (
        echo === Namespace: %%b ===
        for /f "tokens=3 delims=;" %%c in ('docker exec -it aerospike2 asinfo -v "namespace/%%b" ^| findstr "objects="') do (
            echo Objects: %%c
        )
        echo.
    )
) 