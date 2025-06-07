#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_ROOT="my-data-pipeline"
JAVA_FILE="$PROJECT_ROOT/test-runner/src/main/java/com/example/pipeline/RandomOperations.java"

# Function to compile project
compile_project() {
    echo -e "${YELLOW}Đang biên dịch dự án...${NC}"
    
    if [ ! -f "$PROJECT_ROOT/pom.xml" ]; then
        echo -e "${RED}Lỗi: Không tìm thấy file pom.xml tại:${NC}"
        echo "$PROJECT_ROOT/pom.xml"
        exit 1
    fi
    
    echo -e "${YELLOW}Thông tin môi trường:${NC}"
    echo "Đường dẫn hiện tại: $(pwd)"
    echo "Phiên bản Maven: $(mvn -v)"
    echo "Phiên bản Java: $(java -version 2>&1)"
    
    echo -e "\n${YELLOW}Đang biên dịch toàn bộ dự án...${NC}"
    cd "$PROJECT_ROOT"
    mvn clean install -DskipTests
    if [ $? -ne 0 ]; then
        echo -e "${RED}Lỗi khi biên dịch dự án${NC}"
        exit 1
    fi
    cd ..
    
    echo -e "${GREEN}Biên dịch thành công!${NC}"
}

# Function to update operation limit in Java file
update_operation_limit() {
    local limit=$1
    sed -i "s/private static final Map<String, Integer> REGION_LIMITS = Map.of(.*)/private static final Map<String, Integer> REGION_LIMITS = Map.of(\n        \"north\", ${limit},\n        \"central\", ${limit},\n        \"south\", ${limit}\n    );/" "$JAVA_FILE"
    echo -e "${GREEN}Đã cập nhật giới hạn thao tác thành ${limit} cho mỗi region${NC}"
}

# Function to run RandomOperations
run_random_ops() {
    local operation_limit=$1
    
    echo -e "${GREEN}Bắt đầu RandomOperations...${NC}"
    echo "Giới hạn thao tác cho mỗi region: $operation_limit"
    
    update_operation_limit "$operation_limit"
    compile_project
    cd "$PROJECT_ROOT/test-runner" && mvn exec:java -Dexec.mainClass="com.example.pipeline.RandomOperations"
}

# Function to show menu
show_menu() {
    echo -e "${YELLOW}=== MENU THAO TÁC NGẪU NHIÊN ===${NC}"
    echo "1. Chạy với 1000 thao tác/region"
    echo "2. Chạy với 2000 thao tác/region"
    echo "3. Chạy với 5000 thao tác/region"
    echo "4. Chạy với giới hạn thao tác tùy chỉnh"
    echo "0. Thoát"
    echo -e "${YELLOW}===========================${NC}"
}

# Function to run with custom operation limit
run_custom_ops() {
    echo -e "${YELLOW}Nhập giới hạn thao tác cho mỗi region:${NC}"
    read operation_limit
    
    if ! [[ "$operation_limit" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}Lỗi: Vui lòng nhập một số hợp lệ${NC}"
        return
    fi
    
    run_random_ops "$operation_limit"
}

# Main loop
while true; do
    show_menu
    read -p "Chọn một tùy chọn (0-4): " choice
    
    case $choice in
        1)
            run_random_ops 1000
            ;;
        2)
            run_random_ops 2000
            ;;
        3)
            run_random_ops 5000
            ;;
        4)
            run_custom_ops
            ;;
        0)
            echo -e "${GREEN}Tạm biệt!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Lựa chọn không hợp lệ. Vui lòng thử lại.${NC}"
            ;;
    esac
    
    echo -e "\n${YELLOW}Nhấn Enter để tiếp tục...${NC}"
    read
done 