package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.config.RegionConfig;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomInsert {
    private static final String[] SERVICE_TYPES = {
        "MOBILE", "FIXED", "BROADBAND"
    };

    private static final String[] REGIONS = {
        "north", "central", "south"
    };

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Khong the load cau hinh");
            }

            // Lay cau hinh Producer
            String producerHost = config.getProducers().get(0).getHost();
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

            int numThreads = 16;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            int maxRecordsPerRegion = 100_000; // Tổng 300,000 chia cho 3 miền
            AtomicInteger totalCount = new AtomicInteger(0);

            // 🛠 Luồng thực hiện ghi dữ liệu cho từng miền
            for (String region : REGIONS) {
                RegionConfig regionConfig = config.getRegion_groups().get(region);
                if (regionConfig == null) continue;

                // Lấy danh sách tỉnh của miền
                List<String> provinces = new ArrayList<>();
                provinces.addAll(regionConfig.getUnchanged());
                for (List<String> changedProvinces : regionConfig.getChanged().values()) {
                    provinces.addAll(changedProvinces);
                }

                executor.submit(() -> {
                    for (int i = 1; i <= maxRecordsPerRegion; i++) {
                        // Generate UUID for user_id
                        String userId = UUID.randomUUID().toString();
                        
                        // Random phone number
                        String phoneNumber = String.format("09%d", ThreadLocalRandom.current().nextInt(10000000, 100000000));
                        
                        // Random service type
                        String serviceType = SERVICE_TYPES[ThreadLocalRandom.current().nextInt(SERVICE_TYPES.length)];
                        
                        // Random province from this region
                        String province = provinces.get(ThreadLocalRandom.current().nextInt(provinces.size()));

                        // Tạo dữ liệu ngẫu nhiên với kích thước từ 100B đến 1KB
                        byte[] notes = generateRandomBytes(100, 1_000);

                        Key key = new Key(producerNamespace, producerSetName, userId.getBytes());
                        
                        // Create bins with natural data types
                        Bin userIdBin = new Bin("user_id", userId);  // String
                        Bin phoneBin = new Bin("phone", phoneNumber);  // String
                        Bin serviceTypeBin = new Bin("service_type", serviceType);  // String
                        Bin provinceBin = new Bin("province", province);  // String
                        Bin regionBin = new Bin("region", region);  // String
                        Bin lastUpdateBin = new Bin("last_updated", System.currentTimeMillis());  // Long
                        Bin notesBin = new Bin("notes", notes);  // byte[]

                        // Ghi vào Aerospike
                        client.put(policy, key, userIdBin, phoneBin, serviceTypeBin, 
                                 provinceBin, regionBin, lastUpdateBin, notesBin);

                        // Cập nhật bộ đếm
                        totalCount.incrementAndGet();

                        // In tiến trình mỗi 100.000 bản ghi
                        if (totalCount.get() % 100_000 == 0) {
                            System.out.printf("Region %s: Da ghi %d/%d ban ghi%n", 
                                region, totalCount.get(), maxRecordsPerRegion);
                        }
                    }
                });
            }

            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("\nTong so ban ghi da ghi: " + totalCount.get());

            client.close();
        } catch (Exception e) {
            System.err.println("Loi: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static byte[] generateRandomBytes(int minSize, int maxSize) {
        int size = ThreadLocalRandom.current().nextInt(minSize, maxSize + 1);
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
