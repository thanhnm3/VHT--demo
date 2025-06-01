#!/bin/bash

echo "=== Checking Aerospike Server 1 ==="
docker exec -it aerospike-tools aql -h aerospike -c "show sets"
echo

echo "=== Checking Aerospike Server 2 ==="
docker exec -it aerospike-tools aql -h aerospike2 -c "show sets"
echo