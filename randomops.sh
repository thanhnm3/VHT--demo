#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_ROOT="my-data-pipeline"

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

# Function to run RandomOperations with custom parameters
run_random_ops() {
    local ops_per_second=$1
    local thread_pool_size=$2
    
    echo -e "${GREEN}Starting RandomOperations with:${NC}"
    echo "Aerospike: localhost:3000"
    echo "Operations per second: $ops_per_second"
    echo "Thread pool size: $thread_pool_size"
    
    compile_project
    cd "$PROJECT_ROOT/test-runner" && mvn exec:java -Dexec.mainClass="com.example.pipeline.cdc.RandomOperations" \
        -Dexec.args="localhost 3000 test users $ops_per_second $thread_pool_size"
}

# Function to show menu
show_menu() {
    echo -e "${YELLOW}=== RANDOM OPERATIONS MENU ===${NC}"
    echo "1. Run with 100 ops/sec (8 threads)"
    echo "2. Run with 1,000 ops/sec (8 threads)"
    echo "3. Run with 10,000 ops/sec (16 threads)"
    echo "4. Run with custom parameters"
    echo "0. Exit"
    echo -e "${YELLOW}===========================${NC}"
}

# Function to run with custom parameters
run_custom_ops() {
    echo -e "${YELLOW}Enter operations per second:${NC}"
    read ops_per_second
    
    if ! [[ "$ops_per_second" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Error: Please enter a valid number${NC}"
        return
    fi
    
    echo -e "${YELLOW}Enter thread pool size:${NC}"
    read thread_pool_size
    
    if ! [[ "$thread_pool_size" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Error: Please enter a valid number${NC}"
        return
    fi
    
    run_random_ops "$ops_per_second" "$thread_pool_size"
}

# Main loop
while true; do
    show_menu
    read -p "Choose an option (0-4): " choice
    
    case $choice in
        1)
            run_random_ops 100 8
            ;;
        2)
            run_random_ops 1000 8
            ;;
        3)
            run_random_ops 10000 16
            ;;
        4)
            run_custom_ops
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