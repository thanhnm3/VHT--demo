#!/bin/bash

# Màu sắc cho output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Đường dẫn đến file Java
JAVA_FILE="my-data-pipeline/test-runner/src/main/java/com/example/pipeline/RandomInsert.java"
PROJECT_ROOT="my-data-pipeline"

# Kiểm tra file tồn tại
if [ ! -f "$JAVA_FILE" ]; then
    echo -e "${RED}Lỗi: Không tìm thấy file RandomInsert.java tại:${NC}"
    echo "$JAVA_FILE"
    exit 1
fi

# Hàm compile project
compile_project() {
    echo -e "${YELLOW}Đang compile project...${NC}"
    
    # Kiểm tra file pom.xml gốc
    if [ ! -f "$PROJECT_ROOT/pom.xml" ]; then
        echo -e "${RED}Lỗi: Không tìm thấy file pom.xml gốc tại:${NC}"
        echo "$PROJECT_ROOT/pom.xml"
        exit 1
    fi
    
    echo -e "${YELLOW}Thông tin môi trường:${NC}"
    echo "Đường dẫn hiện tại: $(pwd)"
    echo "Maven version: $(mvn -v)"
    echo "Java version: $(java -version 2>&1)"
    
    # Compile toàn bộ project từ thư mục gốc
    echo -e "\n${YELLOW}Compile toàn bộ project...${NC}"
    cd "$PROJECT_ROOT"
    mvn clean install -DskipTests
    if [ $? -ne 0 ]; then
        echo -e "${RED}Lỗi khi compile project${NC}"
        exit 1
    fi
    cd ..
    
    echo -e "${GREEN}Compile thành công!${NC}"
}

# Hàm hiển thị menu
show_menu() {
    echo -e "${YELLOW}=== MENU CHẠY RANDOM INSERT ===${NC}"
    echo "1. Chạy với 100,000 bản ghi (kích thước 100-1000 bytes)"
    echo "2. Chạy với 1,000,000 bản ghi (kích thước 100-1000 bytes)"
    echo "3. Chạy với 100,000 bản ghi (kích thước 500-5000 bytes)"
    echo "4. Chạy với 1,000,000 bản ghi (kích thước 500-5000 bytes)"
    echo "5. Tùy chỉnh thông số"
    echo "0. Thoát"
    echo -e "${YELLOW}===================================${NC}"
}

# Hàm chạy với thông số tùy chỉnh
run_custom() {
    echo -e "${YELLOW}Nhập số lượng bản ghi cho mỗi prefix:${NC}"
    read record_count
    echo -e "${YELLOW}Nhập kích thước tối thiểu (bytes):${NC}"
    read min_size
    echo -e "${YELLOW}Nhập kích thước tối đa (bytes):${NC}"
    read max_size
    
    # Cập nhật file RandomInsert.java với thông số mới
    sed -i "s/int maxRecordsPerPrefix = [0-9_]\+;/int maxRecordsPerPrefix = ${record_count};/" "$JAVA_FILE"
    sed -i "s/byte\[\] personBytes = generateRandomBytes([0-9]\+,[ ]*[0-9]\+);/byte\[\] personBytes = generateRandomBytes(${min_size}, ${max_size});/" "$JAVA_FILE"
    
    echo -e "${GREEN}Đã cập nhật thông số. Bắt đầu chạy...${NC}"
    compile_project
    cd "$PROJECT_ROOT/test-runner" && mvn exec:java -Dexec.mainClass="com.example.pipeline.RandomInsert"
}

# Hàm chạy với thông số cố định
run_with_params() {
    local record_count=$1
    local min_size=$2
    local max_size=$3
    
    # Cập nhật file RandomInsert.java
    sed -i "s/int maxRecordsPerPrefix = [0-9_]\+;/int maxRecordsPerPrefix = ${record_count};/" "$JAVA_FILE"
    sed -i "s/byte\[\] personBytes = generateRandomBytes([0-9]\+,[ ]*[0-9]\+);/byte\[\] personBytes = generateRandomBytes(${min_size}, ${max_size});/" "$JAVA_FILE"
    
    echo -e "${GREEN}Đã cập nhật thông số. Bắt đầu chạy...${NC}"
    compile_project
    cd "$PROJECT_ROOT/test-runner" && mvn exec:java -Dexec.mainClass="com.example.pipeline.RandomInsert"
}

# Main loop
while true; do
    show_menu
    read -p "Chọn một tùy chọn (0-5): " choice
    
    case $choice in
        1)
            run_with_params 100000 100 1000
            ;;
        2)
            run_with_params 1000000 100 1000
            ;;
        3)
            run_with_params 100000 500 5000
            ;;
        4)
            run_with_params 1000000 500 5000
            ;;
        5)
            run_custom
            ;;
        0)
            echo -e "${GREEN}Tạm biệt!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Lựa chọn không hợp lệ. Vui lòng chọn lại.${NC}"
            ;;
    esac
    
    echo -e "\n${YELLOW}Nhấn Enter để tiếp tục...${NC}"
    read
done 