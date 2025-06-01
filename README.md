# VHT--demo

## Giới thiệu

**VHT--demo** là dự án xây dựng hệ thống truyền và đồng bộ dữ liệu giữa các cụm cơ sở dữ liệu phân tán, sử dụng Aerospike làm hệ quản trị dữ liệu và Kafka làm nền tảng truyền tải theo thời gian thực. Dự án giải quyết nhu cầu hợp nhất dữ liệu giữa các hệ thống độc lập, đặc biệt trong bối cảnh các tỉnh/thành phố thực hiện sáp nhập đơn vị hành chính – nơi yêu cầu đồng bộ thông tin thuê bao, dịch vụ và lịch sử giao dịch là vô cùng quan trọng trong ngành viễn thông.

## Kiến trúc hệ thống

Hệ thống gồm 3 thành phần chính:

1. **Cụm cơ sở dữ liệu nguồn**  
   - Aerospike cluster tại một tỉnh/thành.
2. **Tầng trung gian truyền tải**  
   - Kafka cluster làm hàng đợi phân tán, truyền tải dữ liệu theo thời gian thực.
3. **Cụm cơ sở dữ liệu đích**  
   - Aerospike cluster tại tỉnh/thành phố hợp nhất hoặc mới.

Luồng dữ liệu:
- Sử dụng Change Data Capture (CDC) để phát hiện và trích xuất thay đổi dữ liệu từ Aerospike nguồn.
- Dữ liệu thay đổi (event) được gửi tới Kafka để truyền tải an toàn, tin cậy, không gián đoạn.
- Từ Kafka, dữ liệu được tiêu thụ, phân luồng và ghi vào Aerospike đích theo logic cấu hình (ví dụ: đầu số thuê bao, vùng địa lý, loại dịch vụ).
- Toàn bộ hệ thống được giám sát hiệu năng (TPS, latency, lỗi) qua Prometheus và Grafana.

## Đặc điểm nổi bật

- **Đồng bộ dữ liệu theo thời gian thực** giữa các hệ thống phân tán.
- **Kiểm soát chặt chẽ** luồng dữ liệu, hạn chế mất mát, trùng lặp hoặc sai lệch thông tin.
- **Hiệu năng cao, độ trễ thấp, khả năng chịu lỗi tốt** nhờ tận dụng sức mạnh của Kafka và Aerospike.
- **Cấu hình linh hoạt, dễ mở rộng** cho các trường hợp hợp nhất phức tạp.
- **Tích hợp giám sát trực quan** với Prometheus & Grafana.

## Cấu trúc thư mục

```
.
├── .dockerignore               # File cấu hình Docker để bỏ qua các file/thư mục không cần thiết
├── .gitignore                  # File cấu hình Git để bỏ qua các file/thư mục không theo dõi
├── .vscode/                    # Cấu hình cho Visual Studio Code 
├── README.md                   # Tài liệu hướng dẫn chính của repository
├── backup/                     # Chứa các bản sao lưu hoặc cấu hình mẫu, ví dụ như thiết lập Kafka và Prometheus
├── docker/                     # Thư mục chứa các file Docker Compose hoặc script liên quan đến Docker
├── service/                    # Source code chính của hệ thống, bao gồm CDC, producer, và consumer
├── test/                       # Thư mục chứa các kịch bản kiểm thử hoặc tài liệu liên quan đến kiểm tra hệ thống
```

### Chi tiết thư mục

1. **`.dockerignore` và `.gitignore`**  
   - Cấu hình để bỏ qua các file/thư mục không cần thiết khi làm việc với Docker và Git.

2. **`.vscode/`**  
   - Tùy chọn cấu hình cho Visual Studio Code, giúp tối ưu hóa môi trường phát triển.

3. **`backup/`**  
   - Chứa các file mẫu hoặc sao lưu, ví dụ:
     - Cấu hình để triển khai Kafka, Zookeeper, Prometheus trên Kubernetes.
     - Các script để thiết lập môi trường Minikube.

4. **`docker/`**  
   - Chứa các file `docker-compose.yml` và script liên quan để khởi chạy nhanh các thành phần hệ thống bằng Docker.

5. **`service/`**  
   - Source code chính của hệ thống:
     - CDC từ Aerospike.
     - Producer và Consumer để xử lý dữ liệu từ Kafka.

6. **`test/`**  
   - Kịch bản kiểm thử hoặc các file liên quan đến việc kiểm tra hiệu năng hệ thống.

## Hướng dẫn triển khai cơ bản

### 1. Khởi tạo các dịch vụ cơ bản

- Khởi tạo Aerospike, Kafka, Zookeeper, Prometheus, Grafana bằng Docker Compose hoặc Kubernetes.
- Tham khảo cấu hình mẫu trong thư mục `docker/` hoặc `backup/minikube/demo/`.

### 2. Cấu hình CDC cho Aerospike

- Cài đặt và cấu hình module CDC cho Aerospike.
- Trỏ CDC output về Kafka cluster.

### 3. Triển khai Consumer từ Kafka sang Aerospike đích

- Viết hoặc sử dụng consumer sẵn có (Java).
- Cấu hình logic điều hướng dữ liệu phù hợp.

### 4. Giám sát hệ thống

- Deploy Prometheus & Grafana.
- Import dashboard mẫu để theo dõi TPS, latency, error rate...

### 5. Ví dụ triển khai với Docker

```sh

```

Sau khi các dịch vụ hoạt động, truy cập Prometheus hoặc Grafana để xem dashboard giám sát.

## Đóng góp & liên hệ

Mọi ý kiến đóng góp hoặc báo lỗi xin gửi issue trực tiếp trên GitHub hoặc liên hệ  
**Chủ nhiệm đề tài:** thanhnm3

---

**Lưu ý:**  
- Hãy điều chỉnh thông tin và hướng dẫn triển khai cho phù hợp môi trường thực tế.
- Repo này mang tính chất demo, không khuyến cáo sử dụng trực tiếp cho hệ thống production.