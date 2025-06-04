package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomInsert {
    private static final String[] PHONE_PREFIXES = {
        "096", "033"
    };

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Khong the load cau hinh");
            }

            // Lay cau hinh Producer
            String producerHost = "localhost"; // Hardcode host to localhost
            int producerPort = config.getProducers().get(0).getPort();
            String producerNamespace = config.getProducers().get(0).getNamespace();
            String producerSetName = config.getProducers().get(0).getSet();

            System.out.println("=== Bat dau Random Insert ===");
            System.out.println("Producer Host: " + producerHost);
            System.out.println("Producer Port: " + producerPort);
            System.out.println("Producer Namespace: " + producerNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("===============================");

            // Kết nối đến Aerospike
            AerospikeClient client = new AerospikeClient(producerHost, producerPort);
            System.out.println("Ket noi den Aerospike thanh cong!");
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;

            int numThreads = 8; // Số lượng luồng song song (mỗi luồng xử lý 1 prefix)
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            int maxRecordsPerPrefix = 100000; // 200 000 bản ghi cho mỗi prefix
            AtomicInteger totalCount = new AtomicInteger(0); // Tổng số bản ghi đã insert

            // 🛠 Luồng thực hiện ghi dữ liệu
            for (String prefix : PHONE_PREFIXES) {
                executor.submit(() -> {
                    for (int i = 1; i <= maxRecordsPerPrefix; i++) {
                        String phoneNumber = String.format("%s%07d", prefix, i); // Tạo số điện thoại tăng dần
                        byte[] phoneBytes = phoneNumber.getBytes();

                        // 🟢 Tạo dữ liệu ngẫu nhiên với kích thước từ 100B đến 1KB
                        byte[] personBytes = generateRandomBytes(100, 1_000);

                        Key key = new Key(producerNamespace, producerSetName, phoneBytes);
                        Bin personBin = new Bin("personData", personBytes);

                        // 🟢 Giữ lastUpdate ở dạng timestamp
                        Bin lastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis());

                        // 🟢 Ghi vào Aerospike
                        client.put(policy, key, personBin, lastUpdateBin);

                        // Cập nhật bộ đếm
                        totalCount.incrementAndGet();

                        // In tiến trình mỗi 100.000 bản ghi
                        if (i % 100_000 == 0) {
                            System.out.printf("Prefix %s: Da ghi %d/%d ban ghi%n", prefix, i, maxRecordsPerPrefix);
                        }
                    }
                });
            }

            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.HOURS); // Đợi tất cả các luồng hoàn thành
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("\nTong so ban ghi da ghi: " + totalCount.get());

            // Đóng kết nối
            client.close();
        } catch (Exception e) {
            System.err.println("Loi: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Phương thức để tạo byte array với kích thước ngẫu nhiên từ minSize đến maxSize bytes
    private static byte[] generateRandomBytes(int minSize, int maxSize) {
        int size = ThreadLocalRandom.current().nextInt(minSize, maxSize + 1);
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
