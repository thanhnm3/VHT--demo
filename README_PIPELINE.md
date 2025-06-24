# VHT Data Pipeline - Docker Setup

## Chạy Pipeline với Resource Limits

### Resource Configuration
- **CPU**: 2 cores (giới hạn tối đa)
- **Memory**: 4GB RAM (giới hạn tối đa)
- **JVM**: Tối ưu cho 4GB RAM với G1GC

### Cấu hình Container
Pipeline sẽ kết nối với các container sau:
- **Aerospike Producer**: `aerospike:3000`
- **Aerospike Consumer**: `aerospike2:3000`
- **Kafka Brokers**: `source-kafka:29092`, `source-kafka-2:29092`

### Cách chạy

#### 1. Đảm bảo các service cần thiết đang chạy
```bash
cd docker
docker-compose up -d
```

#### 2. Build và chạy pipeline với config Docker
```bash
docker-compose -f docker-compose-pipeline.yml up --build
```

#### 3. Hoặc chạy với Docker trực tiếp
```bash
cd docker
docker build -t vht-pipeline .
docker run --rm --name vht-data-pipeline \
  --cpus=2.0 --memory=4g \
  -e CONFIG_FILE=config-docker.yaml \
  -v $(pwd)/../my-data-pipeline/common/src/main/resources/config-docker.yaml:/app/config-docker.yaml \
  --network kafka-platform \
  vht-pipeline
```

### Kiểm tra
```bash
# Xem logs
docker logs vht-data-pipeline

# Xem resource usage
docker stats vht-data-pipeline

# Kiểm tra kết nối network
docker network inspect kafka-platform
```

### Dừng
```bash
# Nếu dùng docker-compose
docker-compose -f docker-compose-pipeline.yml down

# Nếu dùng docker trực tiếp
docker stop vht-data-pipeline
```

### Lưu ý
- Ứng dụng sẽ tự động chạy MainAll.java khi container start
- JVM được cấu hình tối ưu cho 4GB RAM
- Container sẽ restart tự động nếu bị crash
- Config file `config-docker.yaml` sử dụng tên container thay vì localhost
- Pipeline sẽ đợi các service Aerospike và Kafka khởi động xong mới bắt đầu

### Troubleshooting

#### Lỗi NoClassDefFoundError (SLF4J, SnakeYAML, etc.)
Nếu gặp lỗi `NoClassDefFoundError`, đây là do JAR file không chứa đầy đủ dependencies:
```bash
# Rebuild với no-cache để đảm bảo fat JAR được tạo
docker-compose -f docker-compose-pipeline.yml build --no-cache
```

Hoặc chạy script rebuild:
```bash
rebuild_and_run.bat
```

#### Lỗi kết nối Aerospike/Kafka
- Kiểm tra các container đã chạy: `docker ps`
- Kiểm tra network: `docker network ls`
- Kiểm tra logs của Aerospike: `docker logs aerospike`

#### Lỗi config file
- Đảm bảo file `config-docker.yaml` tồn tại
- Kiểm tra environment variable: `docker exec vht-data-pipeline env | grep CONFIG_FILE` 



### Cách chạy file test kết nối 2 aerospike quay docker 

docker build -f docker/Dockerfile.aerospike-test -t aerospike-test .

docker run --rm --network kafka-platform aerospike-test


### Cách chạy file random insert 
docker build -f docker/Dockerfile.random-insert -t random-insert .

docker run --rm --network kafka-platform random-insert

docker run --rm --network kafka-platform ^
  -v D:/VHT3/VHT--demo/my-data-pipeline/common/src/main/resources/config-docker.yaml:/app/config-docker.yaml ^
  -e CONFIG_FILE=config-docker.yaml ^
  random-insert