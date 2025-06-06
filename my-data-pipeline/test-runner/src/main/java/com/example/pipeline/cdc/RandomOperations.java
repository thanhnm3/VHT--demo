package com.example.pipeline.cdc;

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
    
    // Định nghĩa giới hạn cho từng loại thao tác của mỗi region
    private static final Map<String, Map<String, Integer>> OPERATION_LIMITS = Map.of(
        "north", Map.of(
            "insert", 1000,
            "update", 1000,
            "delete", 1000
        ),
        "central", Map.of(
            "insert", 1000,
            "update", 1000,
            "delete", 1000
        ),
        "south", Map.of(
            "insert", 1000,
            "update", 1000,
            "delete", 1000
        )
    );

    private static final Map<String, Map<String, AtomicInteger>> OPERATION_COUNTERS = new HashMap<>();
    private static final Map<String, Map<String, AtomicLong>> OPERATION_START_TIMES = new HashMap<>();
    private static final Map<String, Map<String, AtomicLong>> OPERATION_LAST_LOG_TIMES = new HashMap<>();

    public static void main(String aeroHost, int aeroPort, String namespace, String setName, int operationsPerSecond,
            int threadPoolSize) {
        // Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient(aeroHost, aeroPort);
        WritePolicy writePolicy = new WritePolicy();
        Policy readPolicy = new Policy();
        writePolicy.sendKey = true;

        Random random = new Random();

        // Khởi tạo bộ đếm và thời gian cho mỗi region và mỗi loại thao tác
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
        }

        // Sử dụng RateLimiter để kiểm soát tốc độ
        RateLimiter rateLimiter = RateLimiter.create(operationsPerSecond);

        AtomicInteger totalInsertCount = new AtomicInteger(0);
        AtomicInteger totalUpdateCount = new AtomicInteger(0);
        AtomicInteger totalDeleteCount = new AtomicInteger(0);

        // Lấy danh sách key từ database
        ConcurrentLinkedQueue<Key> randomKeys = getRandomKeysFromDatabase(client, namespace, setName, KEY_LIMIT);
        System.out.println("Da lay " + randomKeys.size() + " keys tu database");

        // Tạo thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

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
                if (OPERATION_COUNTERS.get(region).get(operationName).get() >= OPERATION_LIMITS.get(region).get(operationName)) {
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
                        operationPerformed = performUpdate(client, writePolicy, readPolicy, namespace, setName, randomKeys, random, region);
                        if (operationPerformed) {
                            totalUpdateCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("update").incrementAndGet();
                            logOperationRate(region, "update");
                        }
                        break;
                    case 2: // Delete
                        operationPerformed = performDelete(client, namespace, setName, randomKeys, region);
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
                    if (counter.get() < OPERATION_LIMITS.get(region).get(operationName)) {
                        allOperationsReachedLimit = false;
                        break;
                    }
                }
                if (!allOperationsReachedLimit) break;
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            System.err.println("Luong bi ngat: " + e.getMessage());
        }

        // In kết quả cuối cùng
        printFinalResults(totalInsertCount, totalUpdateCount, totalDeleteCount, randomKeys.size(), threadPoolSize, operationsPerSecond);

        client.close();
    }

    private static void logOperationRate(String region, String operation) {
        long currentTime = System.currentTimeMillis();
        long lastLogTime = OPERATION_LAST_LOG_TIMES.get(region).get(operation).get();
        
        // Log mỗi giây
        if (currentTime - lastLogTime >= 1000) {
            int count = OPERATION_COUNTERS.get(region).get(operation).get();
            long startTime = OPERATION_START_TIMES.get(region).get(operation).get();
            double opsPerSecond = (count * 1000.0) / (currentTime - startTime);
            
            System.out.printf("[%s] %s: %d thao tac, %.2f ops/s\n", 
                region.toUpperCase(), 
                operation.toUpperCase(), 
                count, 
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
                int limit = OPERATION_LIMITS.get(region).get(operation);
                long startTime = OPERATION_START_TIMES.get(region).get(operation).get();
                long endTime = System.currentTimeMillis();
                double opsPerSecond = (count * 1000.0) / (endTime - startTime);
                
                System.out.printf("%s: %d/%d (%.1f%%) - %.2f ops/s\n", 
                    operation.toUpperCase(), 
                    count, 
                    limit,
                    (count * 100.0) / limit,
                    opsPerSecond);
            }
            
            int regionTotal = operations.get("insert").get() + 
                            operations.get("update").get() + 
                            operations.get("delete").get();
            int regionLimit = OPERATION_LIMITS.get(region).get("insert") + 
                            OPERATION_LIMITS.get(region).get("update") + 
                            OPERATION_LIMITS.get(region).get("delete");
            System.out.printf("Tong cong: %d/%d thao tac (%.1f%%)\n", 
                regionTotal, regionLimit, (regionTotal * 100.0) / regionLimit);
        }

        System.out.println("\n3. Thong tin khac:");
        System.out.println("--------------------------------");
        System.out.println("So luong key da lay tu database: " + keyCount);
        System.out.println("So luong thread da su dung: " + threadPoolSize);
        System.out.println("Toc do thao tac: " + operationsPerSecond + " ops/s");
        System.out.println("================================================");
    }

    private static boolean performInsert(AerospikeClient client, WritePolicy writePolicy, String namespace, String setName,
            Random random, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("insert").get() >= OPERATION_LIMITS.get(region).get("insert")) {
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
            String namespace, String setName, ConcurrentLinkedQueue<Key> randomKeys, Random random, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("update").get() >= OPERATION_LIMITS.get(region).get("update")) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de cap nhat.");
            return false;
        }

        try {
            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(readPolicy, randomKey);
            if (record == null) {
                // Bản ghi không tồn tại, không thực hiện update
                randomKeys.offer(randomKey);
                return false;
            }

            // Lấy region từ trường region của bản ghi
            String recordRegion = record.getString("region");
            if (recordRegion == null || !recordRegion.equals(region)) {
                // Region không khớp, không thực hiện update
                randomKeys.offer(randomKey);
                return false;
            }

            // Chỉ cập nhật notes
            byte[] notes = generateRandomBytes(random, 100, 1_000);
            client.put(writePolicy, randomKey, new Bin("notes", notes));
            return true;
        } catch (AerospikeException e) {
            System.err.println("Loi khi cap nhat ban ghi voi key: " + randomKey.userKey + " (loi: " + e.getMessage() + ")");
        } finally {
            // Luôn trả key về queue
            randomKeys.offer(randomKey);
        }
        return false;
    }

    private static boolean performDelete(AerospikeClient client, String namespace, String setName,
            ConcurrentLinkedQueue<Key> randomKeys, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("delete").get() >= OPERATION_LIMITS.get(region).get("delete")) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de xoa.");
            return false;
        }

        try {
            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(null, randomKey);
            if (record == null) {
                // Bản ghi không tồn tại, không thực hiện delete
                randomKeys.offer(randomKey);
                return false;
            }

            // Lấy region từ trường region của bản ghi
            String recordRegion = record.getString("region");
            if (recordRegion == null || !recordRegion.equals(region)) {
                // Region không khớp, không thực hiện delete
                randomKeys.offer(randomKey);
                return false;
            }

            // Chỉ set notes thành null
            client.put(null, randomKey, Bin.asNull("notes"));
            return true;
        } catch (AerospikeException e) {
            System.err.println("Loi khi xoa ban ghi voi key: " + randomKey.userKey + " (loi: " + e.getMessage() + ")");
        } finally {
            // Luôn trả key về queue
            randomKeys.offer(randomKey);
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

    private static ConcurrentLinkedQueue<Key> getRandomKeysFromDatabase(AerospikeClient client, String namespace,
            String setName, int limit) {
        ConcurrentLinkedQueue<Key> keys = new ConcurrentLinkedQueue<>();
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.setMaxRecords(limit);

        Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(setName);

        try (RecordSet recordSet = client.query(queryPolicy, statement)) {
            while (recordSet.next() && keys.size() < limit) {
                keys.add(recordSet.getKey());
            }
        } catch (AerospikeException e) {
            System.err.println("Loi khi lay danh sach key: " + e.getMessage());
        }

        return keys;
    }
}
