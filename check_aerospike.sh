#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_ROOT="my-data-pipeline"
JAVA_FILE="$PROJECT_ROOT/test-runner/src/main/java/com/example/pipeline/DataVerifier.java"

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
            ns_info=$(docker exec aerospike asinfo -v "namespace/$namespace")
            objects=$(echo "$ns_info" | grep "objects=" | cut -d'=' -f2 | cut -d';' -f1)
            master_objects=$(echo "$ns_info" | grep "master_objects=" | cut -d'=' -f2 | cut -d';' -f1)
            prole_objects=$(echo "$ns_info" | grep "prole_objects=" | cut -d'=' -f2 | cut -d';' -f1)
            replication_factor=$(echo "$ns_info" | grep "replication-factor=" | cut -d'=' -f2 | cut -d';' -f1)
            
            echo "Total Objects: $objects"
            echo "Master Objects: $master_objects"
            echo "Replica Objects: $prole_objects"
            echo "Replication Factor: $replication_factor"
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
            ns_info=$(docker exec aerospike2 asinfo -v "namespace/$namespace")
            objects=$(echo "$ns_info" | grep "objects=" | cut -d'=' -f2 | cut -d';' -f1)
            master_objects=$(echo "$ns_info" | grep "master_objects=" | cut -d'=' -f2 | cut -d';' -f1)
            prole_objects=$(echo "$ns_info" | grep "prole_objects=" | cut -d'=' -f2 | cut -d';' -f1)
            replication_factor=$(echo "$ns_info" | grep "replication-factor=" | cut -d'=' -f2 | cut -d';' -f1)
            
            echo "Total Objects: $objects"
            echo "Master Objects: $master_objects"
            echo "Replica Objects: $prole_objects"
            echo "Replication Factor: $replication_factor"
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
    local record_count=$1
    
    # Update DataVerifier.java with new record count
    sed -i "s/for (int i = 1; i <= [0-9_]\+; i++)/for (int i = 1; i <= ${record_count}; i++)/" "$JAVA_FILE"
    
    echo -e "${GREEN}Updated record count. Starting verification...${NC}"
    compile_project
    cd "$PROJECT_ROOT/test-runner" && mvn exec:java -Dexec.mainClass="com.example.pipeline.DataVerifier"
}

# Function to run DataVerifier directly without compilation
run_direct() {
    cd "$PROJECT_ROOT/test-runner" && mvn exec:java -Dexec.mainClass="com.example.pipeline.DataVerifier"
}

# Function to show menu
show_menu() {
    echo -e "${YELLOW}=== DATA VERIFICATION MENU ===${NC}"
    echo "1. Check Aerospike Total Records"
    echo "2. Verify 1,000 records (default)"
    echo "3. Verify 10,000 records"
    echo "4. Verify 50,000 records"
    echo "5. Verify custom number of records"
    echo "6. Run verification without compilation"
    echo "0. Exit"
    echo -e "${YELLOW}===========================${NC}"
}

# Function to run with custom record count
run_custom_verifier() {
    echo -e "${YELLOW}Enter number of records to verify:${NC}"
    read record_count
    
    if ! [[ "$record_count" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Error: Please enter a valid number${NC}"
        return
    fi
    
    run_data_verifier "$record_count"
}

# Main loop
while true; do
    show_menu
    read -p "Choose an option (0-6): " choice
    
    case $choice in
        1)
            check_aerospike_totals
            ;;
        2)
            run_data_verifier 1000
            ;;
        3)
            run_data_verifier 10000
            ;;
        4)
            run_data_verifier 50000
            ;;
        5)
            run_custom_verifier
            ;;
        6)
            run_direct
            ;;
        0)
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