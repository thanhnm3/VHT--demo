#!/bin/bash

echo "=== Checking Aerospike Server 1 ==="
echo "Namespaces and Sets:"
output=$(docker exec -it aerospike-tools aql -h aerospike -c "show sets" | tr -d '\r')
if [[ "$output" == *"0 rows in set"* ]]; then
    echo "No sets found in Aerospike Server 1."
else
    echo "$output" | awk '
    BEGIN { FS="|"; OFS=""; }
    NR > 2 && $7 ~ /[a-zA-Z0-9_]+/ && $8 ~ /[0-9]+/ && $9 ~ /[a-zA-Z0-9_]+/ {
        gsub(/ /, "", $7); # Xóa khoảng trắng trong namespace
        gsub(/ /, "", $9); # Xóa khoảng trắng trong set
        gsub(/ /, "", $8); # Xóa khoảng trắng trong objects
        print "Namespace: " $7 ", Set: " $9 ", Objects: " $8;
    }'
fi
echo

echo "=== Checking Aerospike Server 2 ==="
echo "Namespaces and Sets:"
output=$(docker exec -it aerospike-tools aql -h aerospike2 -c "show sets" | tr -d '\r')
if [[ "$output" == *"0 rows in set"* ]]; then
    echo "No sets found in Aerospike Server 2."
else
    echo "$output" | awk '
    BEGIN { FS="|"; OFS=""; }
    NR > 2 && $7 ~ /[a-zA-Z0-9_]+/ && $8 ~ /[0-9]+/ && $9 ~ /[a-zA-Z0-9_]+/ {
        gsub(/ /, "", $7); # Xóa khoảng trắng trong namespace
        gsub(/ /, "", $9); # Xóa khoảng trắng trong set
        gsub(/ /, "", $8); # Xóa khoảng trắng trong objects
        print "Namespace: " $7 ", Set: " $9 ", Objects: " $8;
    }'
fi
echo