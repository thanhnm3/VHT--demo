#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_ROOT="my-data-pipeline"

# Function to check Aerospike totals
check_aerospike_totals() {
    echo -e "${YELLOW}=== Checking Aerospike Server 1 (Producer) ===${NC}"
    echo "Namespaces:"
    namespaces=$(docker exec aerospike asinfo -v "namespaces")
    echo "$namespaces"
    echo

    echo "Objects in each namespace:"
    producer_total=0
    # Split namespaces by semicolon and process each
    IFS=';' read -ra NS_ARRAY <<< "$namespaces"
    for namespace in "${NS_ARRAY[@]}"; do
        namespace=$(echo "$namespace" | tr -d ' ')
        if [ -n "$namespace" ]; then
            echo "=== Namespace: $namespace ==="
            objects=$(docker exec aerospike asinfo -v "namespace/$namespace" | grep "objects=" | cut -d';' -f3)
            echo "Objects: $objects"
            producer_total=$((producer_total + objects))
            echo
        fi
    done

    echo -e "${YELLOW}=== Checking Aerospike Server 2 (Consumer) ===${NC}"
    echo "Namespaces:"
    namespaces=$(docker exec aerospike2 asinfo -v "namespaces")
    echo "$namespaces"
    echo

    echo "Objects in each namespace:"
    consumer_total=0
    # Split namespaces by semicolon and process each
    IFS=';' read -ra NS_ARRAY <<< "$namespaces"
    for namespace in "${NS_ARRAY[@]}"; do
        namespace=$(echo "$namespace" | tr -d ' ')
        if [ -n "$namespace" ]; then
            echo "=== Namespace: $namespace ==="
            objects=$(docker exec aerospike2 asinfo -v "namespace/$namespace" | grep "objects=" | cut -d';' -f3)
            echo "Objects: $objects"
            consumer_total=$((consumer_total + objects))
            echo
        fi
    done

    echo -e "${YELLOW}=== Record Count Comparison ===${NC}"
    echo "Producer Total Records: $producer_total"
    echo "Consumer Total Records: $consumer_total"
    
    if [ "$producer_total" -eq "$consumer_total" ]; then
        echo -e "${GREEN}Status: MATCHED${NC}"
    else
        echo -e "${RED}Status: MISMATCHED${NC}"
        diff=$((producer_total - consumer_total))
        echo "Difference: $diff records"
    fi
}

# Function to compile project
compile_project() {
    echo -e "${YELLOW}Compiling project...${NC}"
    
    if [ ! -f "$PROJECT_ROOT/pom.xml" ]; then
        echo -e "${RED}Error: pom.xml not found at:${NC}"
        echo "$PROJECT_ROOT/pom.xml"
        exit 1
    fi
    
    echo -e "${YELLOW}Environment information:${NC}"
    echo "Current path: $(pwd)"
    echo "Maven version: $(mvn -v)"
    echo "Java version: $(java -version 2>&1)"
    
    echo -e "\n${YELLOW}Compiling entire project...${NC}"
    cd "$PROJECT_ROOT"
    mvn clean install -DskipTests
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error compiling project${NC}"
        exit 1
    fi
    cd ..
    
    echo -e "${GREEN}Compilation successful!${NC}"
}

# Function to run DataVerifier
run_data_verifier() {
    echo -e "${GREEN}Starting verification...${NC}"
    compile_project
    cd "$PROJECT_ROOT/test-runner" && mvn exec:java -Dexec.mainClass="com.example.pipeline.full.DataVerifier"
}

# Function to show menu
show_menu() {
    echo -e "${YELLOW}=== AEROSPIKE CHECK MENU ===${NC}"
    echo "1. Check Aerospike Total Records"
    echo "2. Run DataVerifier"
    echo "3. Exit"
    echo -e "${YELLOW}===========================${NC}"
}

# Main loop
while true; do
    show_menu
    read -p "Choose an option (1-3): " choice
    
    case $choice in
        1)
            check_aerospike_totals
            ;;
        2)
            run_data_verifier
            ;;
        3)
            echo -e "${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid choice. Please try again.${NC}"
            ;;
    esac
    
    echo -e "\n${YELLOW}Press Enter to continue...${NC}"
    read
done