package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.google.common.util.concurrent.RateLimiter;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RandomOperations {
    private static final String[] REGIONS = {
        "north", "central", "south"
    };
    private static final int MAX_RECORDS_PER_REGION = 200_000;
    private static final int KEY_LIMIT = 30_000;
    
    // Định nghĩa giới hạn cho từng region
    private static final Map<String, Integer> REGION_LIMITS = Map.of(
        "north", 1000,
        "central", 1000,
        "south", 1000
    );

    private static final Map<String, Map<String, AtomicInteger>> OPERATION_COUNTERS = new HashMap<>();
    private static final Map<String, Map<String, AtomicLong>> OPERATION_START_TIMES = new HashMap<>();
    private static final Map<String, Map<String, AtomicLong>> OPERATION_LAST_LOG_TIMES = new HashMap<>();
    private static final Map<String, ConcurrentLinkedQueue<Key>> KEY_POOLS = new HashMap<>();

    public static void main(String[] args) {
        // Hardcoded parameters
        String aeroHost = "localhost";
        int aeroPort = 3000;
        String namespace = "producer";
        String setName = "users";
        int operationsPerSecond = 1000;  // Default value
        int threadPoolSize = 8;          // Default value

        System.out.println("\n=== THONG TIN KET NOI ===");
        System.out.println("Aerospike Host: " + aeroHost);
        System.out.println("Aerospike Port: " + aeroPort);
        System.out.println("Namespace: " + namespace);
        System.out.println("Set Name: " + setName);
        System.out.println("Operations/Second: " + operationsPerSecond);
        System.out.println("Thread Pool Size: " + threadPoolSize);
        System.out.println("========================\n");

        // Kết nối đến Aerospike
        System.out.println("Dang ket noi den Aerospike...");
        AerospikeClient client = new AerospikeClient(aeroHost, aeroPort);
        System.out.println("Ket noi thanh cong!\n");

        WritePolicy writePolicy = new WritePolicy();
        Policy readPolicy = new Policy();
        writePolicy.sendKey = true;

        Random random = new Random();

        // Khởi tạo bộ đếm và thời gian cho mỗi region và mỗi loại thao tác
        System.out.println("Dang khoi tao bo dem va thoi gian...");
        for (String region : REGIONS) {
            Map<String, AtomicInteger> regionCounters = new HashMap<>();
            Map<String, AtomicLong> regionStartTimes = new HashMap<>();
            Map<String, AtomicLong> regionLastLogTimes = new HashMap<>();
            
            for (String operation : new String[]{"insert", "update", "delete"}) {
                regionCounters.put(operation, new AtomicInteger(0));
                regionStartTimes.put(operation, new AtomicLong(System.currentTimeMillis()));
                regionLastLogTimes.put(operation, new AtomicLong(System.currentTimeMillis()));
            }
            
            OPERATION_COUNTERS.put(region, regionCounters);
            OPERATION_START_TIMES.put(region, regionStartTimes);
            OPERATION_LAST_LOG_TIMES.put(region, regionLastLogTimes);
            
            // Khởi tạo key pool cho mỗi region
            KEY_POOLS.put(region, new ConcurrentLinkedQueue<>());
        }
        System.out.println("Khoi tao thanh cong!\n");

        // Sử dụng RateLimiter để kiểm soát tốc độ
        RateLimiter rateLimiter = RateLimiter.create(operationsPerSecond);

        AtomicInteger totalInsertCount = new AtomicInteger(0);
        AtomicInteger totalUpdateCount = new AtomicInteger(0);
        AtomicInteger totalDeleteCount = new AtomicInteger(0);

        // Lấy danh sách key từ database và phân phối vào các pool theo region
        System.out.println("Dang lay danh sach key tu database...");
        initializeKeyPools(client, namespace, setName);
        System.out.println("Da khoi tao key pools cho cac region:");
        for (String region : REGIONS) {
            System.out.printf("- Region %s: %d keys\n", region, KEY_POOLS.get(region).size());
        }
        System.out.println();

        // Tạo thread pool
        System.out.println("Dang khoi tao thread pool...");
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        System.out.println("Thread pool da san sang!\n");

        System.out.println("=== BAT DAU THUC HIEN THAO TAC ===\n");

        // Kiểm tra xem tất cả region và loại thao tác đã đạt giới hạn chưa
        boolean allOperationsReachedLimit = false;
        while (!allOperationsReachedLimit) {
            rateLimiter.acquire();

            executor.submit(() -> {
                int operationType = random.nextInt(3); // 0: Insert, 1: Update, 2: Delete
                String operationName = operationType == 0 ? "insert" : 
                                     operationType == 1 ? "update" : "delete";
                boolean operationPerformed = false;
                
                // Chọn region ngẫu nhiên
                String region = REGIONS[random.nextInt(REGIONS.length)];
                
                // Kiểm tra giới hạn cho region và loại thao tác
                if (OPERATION_COUNTERS.get(region).get(operationName).get() >= REGION_LIMITS.get(region)) {
                    return;
                }
                
                switch (operationType) {
                    case 0: // Insert
                        operationPerformed = performInsert(client, writePolicy, namespace, setName, random, region);
                        if (operationPerformed) {
                            totalInsertCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("insert").incrementAndGet();
                            logOperationRate(region, "insert");
                        }
                        break;
                    case 1: // Update
                        operationPerformed = performUpdate(client, writePolicy, readPolicy, namespace, setName, random, region);
                        if (operationPerformed) {
                            totalUpdateCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("update").incrementAndGet();
                            logOperationRate(region, "update");
                        }
                        break;
                    case 2: // Delete
                        operationPerformed = performDelete(client, namespace, setName, random, region);
                        if (operationPerformed) {
                            totalDeleteCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("delete").incrementAndGet();
                            logOperationRate(region, "delete");
                        }
                        break;
                }
            });

            // Kiểm tra xem tất cả region và loại thao tác đã đạt giới hạn chưa
            allOperationsReachedLimit = true;
            for (Map.Entry<String, Map<String, AtomicInteger>> regionEntry : OPERATION_COUNTERS.entrySet()) {
                String region = regionEntry.getKey();
                Map<String, AtomicInteger> operations = regionEntry.getValue();
                for (Map.Entry<String, AtomicInteger> operationEntry : operations.entrySet()) {
                    String operationName = operationEntry.getKey();
                    AtomicInteger counter = operationEntry.getValue();
                    if (counter.get() < REGION_LIMITS.get(region)) {
                        allOperationsReachedLimit = false;
                        break;
                    }
                }
                if (!allOperationsReachedLimit) break;
            }
        }

        System.out.println("\nDang doi cac thread hoan thanh...");
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            System.err.println("Luong bi ngat: " + e.getMessage());
        }
        System.out.println("Tat ca cac thread da hoan thanh!\n");

        // In kết quả cuối cùng
        printFinalResults(totalInsertCount, totalUpdateCount, totalDeleteCount, getTotalKeysInPools(), threadPoolSize, operationsPerSecond);

        System.out.println("\nDang dong ket noi Aerospike...");
        client.close();
        System.out.println("Ket noi da dong!");
    }

    private static void logOperationRate(String region, String operation) {
        long currentTime = System.currentTimeMillis();
        long lastLogTime = OPERATION_LAST_LOG_TIMES.get(region).get(operation).get();
        
        // Log mỗi giây
        if (currentTime - lastLogTime >= 1000) {
            int count = OPERATION_COUNTERS.get(region).get(operation).get();
            long startTime = OPERATION_START_TIMES.get(region).get(operation).get();
            double opsPerSecond = (count * 1000.0) / (currentTime - startTime);
            
            System.out.printf("[%s] %s: %d/%d thao tac (%.1f%%), %.2f ops/s\n", 
                region.toUpperCase(), 
                operation.toUpperCase(), 
                count,
                REGION_LIMITS.get(region),
                (count * 100.0) / REGION_LIMITS.get(region),
                opsPerSecond);
            
            OPERATION_LAST_LOG_TIMES.get(region).get(operation).set(currentTime);
        }
    }

    private static void printFinalResults(AtomicInteger totalInsertCount, AtomicInteger totalUpdateCount, 
            AtomicInteger totalDeleteCount, int keyCount, int threadPoolSize, int operationsPerSecond) {
        System.out.println("\n=== KET QUA THUC HIEN THAO TAC ===");
        System.out.println("Thoi gian ket thuc: " + java.time.LocalDateTime.now());
        System.out.println("\n1. Tong so thao tac da thuc hien:");
        System.out.println("--------------------------------");
        System.out.printf("Them moi (Insert): %d thao tac\n", totalInsertCount.get());
        System.out.printf("Cap nhat (Update): %d thao tac\n", totalUpdateCount.get());
        System.out.printf("Xoa (Delete): %d thao tac\n", totalDeleteCount.get());
        System.out.printf("Tong cong: %d thao tac\n", 
            totalInsertCount.get() + totalUpdateCount.get() + totalDeleteCount.get());

        System.out.println("\n2. Chi tiet theo tung region:");
        System.out.println("--------------------------------");
        for (Map.Entry<String, Map<String, AtomicInteger>> regionEntry : OPERATION_COUNTERS.entrySet()) {
            String region = regionEntry.getKey();
            Map<String, AtomicInteger> operations = regionEntry.getValue();
            System.out.printf("\nRegion: %s\n", region.toUpperCase());
            System.out.println("--------------------------------");
            
            for (String operation : new String[]{"insert", "update", "delete"}) {
                int count = operations.get(operation).get();
                int limit = REGION_LIMITS.get(region);
                long startTime = OPERATION_START_TIMES.get(region).get(operation).get();
                long endTime = System.currentTimeMillis();
                double opsPerSecond = (count * 1000.0) / (endTime - startTime);
                double duration = (endTime - startTime) / 1000.0;
                
                System.out.printf("%s: %d/%d (%.1f%%) - %.2f ops/s - Thoi gian: %.1f giay\n", 
                    operation.toUpperCase(), 
                    count, 
                    limit,
                    (count * 100.0) / limit,
                    opsPerSecond,
                    duration);
            }
            
            int regionTotal = operations.get("insert").get() + 
                            operations.get("update").get() + 
                            operations.get("delete").get();
            int regionLimit = REGION_LIMITS.get(region) * 3; // Tổng giới hạn cho 3 loại thao tác
            System.out.printf("Tong cong: %d/%d thao tac (%.1f%%)\n", 
                regionTotal, regionLimit, (regionTotal * 100.0) / regionLimit);
        }

        System.out.println("\n3. Thong tin khac:");
        System.out.println("--------------------------------");
        System.out.println("So luong key trong pools: " + keyCount);
        System.out.println("So luong thread da su dung: " + threadPoolSize);
        System.out.println("Toc do thao tac: " + operationsPerSecond + " ops/s");
        System.out.println("================================================");
    }

    private static boolean performInsert(AerospikeClient client, WritePolicy writePolicy, String namespace, String setName,
            Random random, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("insert").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        int number = random.nextInt(MAX_RECORDS_PER_REGION) + 1;
        String phoneNumber = String.format("%s%07d", region, number);
        byte[] phoneBytes = phoneNumber.getBytes();

        Key key = new Key(namespace, setName, phoneBytes);
        
        // Kiểm tra xem key đã tồn tại chưa
        try {
            Record existingRecord = client.get(null, key);
            if (existingRecord != null) {
                // Key đã tồn tại, không thực hiện insert
                return false;
            }
        } catch (AerospikeException e) {
            // Nếu có lỗi khi kiểm tra, không thực hiện insert
            return false;
        }

        // Tạo dữ liệu ngẫu nhiên cho các trường
        String userId = String.format("user_%s_%d", region, number);
        String phone = phoneNumber;
        String serviceType = random.nextBoolean() ? "prepaid" : "postpaid";
        String province = getProvinceForRegion(region);
        long lastUpdated = System.currentTimeMillis();
        byte[] notes = generateRandomBytes(random, 100, 1_000);

        try {
            client.put(writePolicy, key,
                new Bin("user_id", userId),
                new Bin("phone", phone),
                new Bin("service_type", serviceType),
                new Bin("province", province),
                new Bin("region", region),
                new Bin("last_updated", lastUpdated),
                new Bin("notes", notes)
            );
            return true;
        } catch (AerospikeException e) {
            System.err.println("Loi khi them ban ghi voi key: " + key.userKey + " (loi: " + e.getMessage() + ")");
            return false;
        }
    }

    private static boolean performUpdate(AerospikeClient client, WritePolicy writePolicy, Policy readPolicy,
            String namespace, String setName, Random random, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("update").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        Key randomKey = KEY_POOLS.get(region).poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de cap nhat cho region: " + region);
            return false;
        }

        try {
            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(readPolicy, randomKey);
            if (record == null) {
                // Bản ghi không tồn tại, không thực hiện update
                KEY_POOLS.get(region).offer(randomKey);
                return false;
            }

            // Chỉ cập nhật notes
            byte[] notes = generateRandomBytes(random, 100, 1_000);
            client.put(writePolicy, randomKey, new Bin("notes", notes));
            return true;
        } catch (AerospikeException e) {
            System.err.println("Loi khi cap nhat ban ghi voi key: " + randomKey.userKey + " (loi: " + e.getMessage() + ")");
        } finally {
            // Luôn trả key về pool
            KEY_POOLS.get(region).offer(randomKey);
        }
        return false;
    }

    private static boolean performDelete(AerospikeClient client, String namespace, String setName,
            Random random, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("delete").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        Key randomKey = KEY_POOLS.get(region).poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de xoa cho region: " + region);
            return false;
        }

        try {
            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(null, randomKey);
            if (record == null) {
                // Bản ghi không tồn tại, không thực hiện delete
                KEY_POOLS.get(region).offer(randomKey);
                return false;
            }

            // Chỉ set notes thành null
            client.put(null, randomKey, Bin.asNull("notes"));
            return true;
        } catch (AerospikeException e) {
            System.err.println("Loi khi xoa ban ghi voi key: " + randomKey.userKey + " (loi: " + e.getMessage() + ")");
        } finally {
            // Luôn trả key về pool
            KEY_POOLS.get(region).offer(randomKey);
        }
        return false;
    }

    private static String getProvinceForRegion(String region) {
        switch (region) {
            case "north":
                return "Ha Noi";
            case "central":
                return "Da Nang";
            case "south":
                return "Ho Chi Minh";
            default:
                return "Unknown";
        }
    }

    private static byte[] generateRandomBytes(Random random, int minSize, int maxSize) {
        int size = random.nextInt(maxSize - minSize + 1) + minSize;
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static void initializeKeyPools(AerospikeClient client, String namespace, String setName) {
        System.out.println("\n=== KHOI TAO KEY POOLS ===");
        System.out.println("Namespace: " + namespace);
        System.out.println("Set Name: " + setName);
        System.out.println("Key Limit: " + KEY_LIMIT);
        
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.setMaxRecords(KEY_LIMIT);

        Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(setName);

        int totalKeys = 0;
        Map<String, Integer> regionKeyCounts = new HashMap<>();
        for (String region : REGIONS) {
            regionKeyCounts.put(region, 0);
        }

        try (RecordSet recordSet = client.query(queryPolicy, statement)) {
            System.out.println("\nDang lay key tu database...");
            long startTime = System.currentTimeMillis();
            
            while (recordSet.next()) {
                Key key = recordSet.getKey();
                Record record = recordSet.getRecord();
                String region = record.getString("region");
                
                if (region != null && KEY_POOLS.containsKey(region)) {
                    KEY_POOLS.get(region).offer(key);
                    regionKeyCounts.put(region, regionKeyCounts.get(region) + 1);
                    totalKeys++;
                }
            }
            
            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;
            
            System.out.println("\nKet qua lay key:");
            System.out.println("--------------------------------");
            System.out.printf("Tong so key da lay: %d\n", totalKeys);
            System.out.printf("Thoi gian lay key: %.2f giay\n", duration);
            System.out.printf("Toc do lay key: %.2f key/giay\n", totalKeys / duration);
            
            System.out.println("\nChi tiet theo region:");
            System.out.println("--------------------------------");
            for (String region : REGIONS) {
                int count = regionKeyCounts.get(region);
                System.out.printf("- Region %s: %d keys (%.1f%%)\n", 
                    region.toUpperCase(), 
                    count,
                    (count * 100.0) / totalKeys);
            }
            
            if (totalKeys == 0) {
                System.err.println("\nCANH BAO: Khong tim thay key nao trong database!");
                System.err.println("Cac thao tac update va delete se khong the thuc hien.");
            } else if (totalKeys < KEY_LIMIT) {
                System.out.printf("\nLuu y: So luong key thuc te (%d) nho hon KEY_LIMIT (%d)\n", 
                    totalKeys, KEY_LIMIT);
            }
            
        } catch (AerospikeException e) {
            System.err.println("\nLOI: Khong the lay danh sach key tu database!");
            System.err.println("Chi tiet loi: " + e.getMessage());
            System.err.println("Cac thao tac update va delete se khong the thuc hien.");
        }
        
        System.out.println("\n=== KET THUC KHOI TAO KEY POOLS ===\n");
    }

    private static int getTotalKeysInPools() {
        return KEY_POOLS.values().stream()
                .mapToInt(ConcurrentLinkedQueue::size)
                .sum();
    }
}
