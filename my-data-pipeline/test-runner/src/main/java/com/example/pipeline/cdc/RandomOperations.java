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

public class RandomOperations {
    private static final String[] REGIONS = {
        "north", "central", "south"
    };
    private static final int MAX_RECORDS_PER_REGION = 200_000;
    private static final int KEY_LIMIT = 100_000;
    private static final Map<String, Integer> REGION_LIMITS = Map.of(
        "north", 1_000,
        "central", 2_000,
        "south", 1_500
    );
    private static final Map<String, Map<String, AtomicInteger>> OPERATION_COUNTERS = new HashMap<>();

    public static void main(String aeroHost, int aeroPort, String namespace, String setName, int operationsPerSecond,
            int threadPoolSize) {
        // Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient(aeroHost, aeroPort);
        WritePolicy writePolicy = new WritePolicy();
        Policy readPolicy = new Policy();
        writePolicy.sendKey = true;

        Random random = new Random();

        // Khởi tạo bộ đếm cho mỗi region và mỗi loại thao tác
        for (String region : REGIONS) {
            Map<String, AtomicInteger> regionCounters = new HashMap<>();
            regionCounters.put("insert", new AtomicInteger(0));
            regionCounters.put("update", new AtomicInteger(0));
            regionCounters.put("delete", new AtomicInteger(0));
            OPERATION_COUNTERS.put(region, regionCounters);
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
                if (OPERATION_COUNTERS.get(region).get(operationName).get() >= REGION_LIMITS.get(region)) {
                    return;
                }
                
                switch (operationType) {
                    case 0: // Insert
                        operationPerformed = performInsert(client, writePolicy, namespace, setName, random, region);
                        if (operationPerformed) {
                            totalInsertCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("insert").incrementAndGet();
                        }
                        break;
                    case 1: // Update
                        operationPerformed = performUpdate(client, writePolicy, readPolicy, namespace, setName, randomKeys, random, region);
                        if (operationPerformed) {
                            totalUpdateCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("update").incrementAndGet();
                        }
                        break;
                    case 2: // Delete
                        operationPerformed = performDelete(client, namespace, setName, randomKeys, region);
                        if (operationPerformed) {
                            totalDeleteCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("delete").incrementAndGet();
                        }
                        break;
                }
            });

            // Kiểm tra xem tất cả region và loại thao tác đã đạt giới hạn chưa
            allOperationsReachedLimit = true;
            for (Map.Entry<String, Map<String, AtomicInteger>> regionEntry : OPERATION_COUNTERS.entrySet()) {
                String region = regionEntry.getKey();
                Map<String, AtomicInteger> operations = regionEntry.getValue();
                for (AtomicInteger counter : operations.values()) {
                    if (counter.get() < REGION_LIMITS.get(region)) {
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

        System.out.println("\n=== Ket qua thuc hien thao tac ===");
        System.out.println("Tong so thao tac:");
        System.out.println("Them moi: " + totalInsertCount.get());
        System.out.println("Cap nhat: " + totalUpdateCount.get());
        System.out.println("Xoa: " + totalDeleteCount.get());
        System.out.println("\nSo luong thay doi theo region:");
        for (Map.Entry<String, Map<String, AtomicInteger>> regionEntry : OPERATION_COUNTERS.entrySet()) {
            String region = regionEntry.getKey();
            Map<String, AtomicInteger> operations = regionEntry.getValue();
            System.out.printf("  %s:\n", region);
            System.out.printf("    Insert: %d/%d\n", 
                operations.get("insert").get(), REGION_LIMITS.get(region));
            System.out.printf("    Update: %d/%d\n", 
                operations.get("update").get(), REGION_LIMITS.get(region));
            System.out.printf("    Delete: %d/%d\n", 
                operations.get("delete").get(), REGION_LIMITS.get(region));
        }
        System.out.println("================================");

        client.close();
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
            String namespace, String setName, ConcurrentLinkedQueue<Key> randomKeys, Random random, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("update").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de cap nhat.");
            return false;
        }

        try {
            // Lấy region từ key
            String keyStr = new String((byte[])randomKey.userKey.getObject());
            String keyRegion = keyStr.substring(0, 2); // Lấy 2 ký tự đầu cho region
            
            // Chỉ cập nhật nếu region khớp
            if (!keyRegion.equals(region)) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(readPolicy, randomKey);
            if (record == null) {
                // Bản ghi không tồn tại, không thực hiện update
                randomKeys.offer(randomKey);
                return false;
            }

            // Cập nhật các trường
            String serviceType = random.nextBoolean() ? "prepaid" : "postpaid";
            long lastUpdated = System.currentTimeMillis();
            byte[] notes = generateRandomBytes(random, 100, 1_000);

            client.put(writePolicy, randomKey,
                new Bin("service_type", serviceType),
                new Bin("last_updated", lastUpdated),
                new Bin("notes", notes)
            );
            return true;
        } catch (AerospikeException e) {
            System.err.println(
                    "Loi khi cap nhat ban ghi voi key: " + randomKey.userKey + " (loi: " + e.getMessage() + ")");
        } finally {
            randomKeys.offer(randomKey);
        }
        return false;
    }

    private static boolean performDelete(AerospikeClient client, String namespace, String setName,
            ConcurrentLinkedQueue<Key> randomKeys, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("delete").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de xoa.");
            return false;
        }

        try {
            // Lấy region từ key
            String keyStr = new String((byte[])randomKey.userKey.getObject());
            String keyRegion = keyStr.substring(0, 5); // Lấy 5 ký tự đầu cho region (north, south, etc.)
            
            // Chỉ xóa nếu region khớp
            if (!keyRegion.equals(region)) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(null, randomKey);
            if (record == null) {
                // Bản ghi không tồn tại, không thực hiện delete
                randomKeys.offer(randomKey);
                return false;
            }

            // Đánh dấu bản ghi là đã xóa bằng cách set tất cả các trường thành null
            client.put(null, randomKey,
                Bin.asNull("user_id"),
                Bin.asNull("phone"),
                Bin.asNull("service_type"),
                Bin.asNull("province"),
                Bin.asNull("region"),
                Bin.asNull("last_updated"),
                Bin.asNull("notes")
            );
            return true;
        } catch (AerospikeException e) {
            System.err.println(
                    "Loi khi xoa ban ghi voi key: " + randomKey.userKey + " (loi: " + e.getMessage() + ")");
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
