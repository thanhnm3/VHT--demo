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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;
import java.util.HashMap;

public class RandomOperations {
    private static final String[] PHONE_PREFIXES = {
        "096", "033"
    };
    private static final int MAX_RECORDS_PER_PREFIX = 200_000;
    private static final int KEY_LIMIT = 20_000;
    private static final Map<String, Integer> PREFIX_LIMITS = Map.of(
        "033", 1_000,
        "096", 2_000
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

        // Khởi tạo bộ đếm cho mỗi prefix và mỗi loại thao tác
        for (String prefix : PHONE_PREFIXES) {
            Map<String, AtomicInteger> prefixCounters = new HashMap<>();
            prefixCounters.put("insert", new AtomicInteger(0));
            prefixCounters.put("update", new AtomicInteger(0));
            prefixCounters.put("delete", new AtomicInteger(0));
            OPERATION_COUNTERS.put(prefix, prefixCounters);
        }

        // Sử dụng RateLimiter để kiểm soát tốc độ
        RateLimiter rateLimiter = RateLimiter.create(operationsPerSecond);

        AtomicInteger totalInsertCount = new AtomicInteger(0);
        AtomicInteger totalUpdateCount = new AtomicInteger(0);
        AtomicInteger totalDeleteCount = new AtomicInteger(0);

        AtomicInteger insertCountThisSecond = new AtomicInteger(0);
        AtomicInteger updateCountThisSecond = new AtomicInteger(0);
        AtomicInteger deleteCountThisSecond = new AtomicInteger(0);

        // Lấy danh sách key từ database
        ConcurrentLinkedQueue<Key> randomKeys = getRandomKeysFromDatabase(client, namespace, setName, KEY_LIMIT);
        System.out.println("Da lay " + randomKeys.size() + " keys tu database");

        // Tạo thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

        // Scheduler để in log mỗi giây
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("Them moi: " + insertCountThisSecond.get() +
                    ", Cap nhat: " + updateCountThisSecond.get() +
                    ", Xoa: " + deleteCountThisSecond.get());
            System.out.println("So luong thay doi theo prefix va loai thao tac:");
            for (Map.Entry<String, Map<String, AtomicInteger>> prefixEntry : OPERATION_COUNTERS.entrySet()) {
                String prefix = prefixEntry.getKey();
                Map<String, AtomicInteger> operations = prefixEntry.getValue();
                System.out.printf("  %s:\n", prefix);
                System.out.printf("    Insert: %d/%d\n", 
                    operations.get("insert").get(), PREFIX_LIMITS.get(prefix));
                System.out.printf("    Update: %d/%d\n", 
                    operations.get("update").get(), PREFIX_LIMITS.get(prefix));
                System.out.printf("    Delete: %d/%d\n", 
                    operations.get("delete").get(), PREFIX_LIMITS.get(prefix));
            }
            insertCountThisSecond.set(0);
            updateCountThisSecond.set(0);
            deleteCountThisSecond.set(0);
        }, 0, 1, TimeUnit.SECONDS);

        // Kiểm tra xem tất cả prefix và loại thao tác đã đạt giới hạn chưa
        boolean allOperationsReachedLimit = false;
        while (!allOperationsReachedLimit) {
            rateLimiter.acquire();

            executor.submit(() -> {
                int operationType = random.nextInt(3); // 0: Insert, 1: Update, 2: Delete
                String operationName = operationType == 0 ? "insert" : 
                                     operationType == 1 ? "update" : "delete";
                boolean operationPerformed = false;
                
                // Chọn prefix ngẫu nhiên
                String prefix = PHONE_PREFIXES[random.nextInt(PHONE_PREFIXES.length)];
                
                // Kiểm tra giới hạn cho prefix và loại thao tác
                if (OPERATION_COUNTERS.get(prefix).get(operationName).get() >= PREFIX_LIMITS.get(prefix)) {
                    return;
                }
                
                switch (operationType) {
                    case 0: // Insert
                        operationPerformed = performInsert(client, writePolicy, namespace, setName, random, prefix);
                        if (operationPerformed) {
                            totalInsertCount.incrementAndGet();
                            insertCountThisSecond.incrementAndGet();
                            OPERATION_COUNTERS.get(prefix).get("insert").incrementAndGet();
                        }
                        break;
                    case 1: // Update
                        operationPerformed = performUpdate(client, writePolicy, readPolicy, namespace, setName, randomKeys, random, prefix);
                        if (operationPerformed) {
                            totalUpdateCount.incrementAndGet();
                            updateCountThisSecond.incrementAndGet();
                            OPERATION_COUNTERS.get(prefix).get("update").incrementAndGet();
                        }
                        break;
                    case 2: // Delete
                        operationPerformed = performDelete(client, namespace, setName, randomKeys, prefix);
                        if (operationPerformed) {
                            totalDeleteCount.incrementAndGet();
                            deleteCountThisSecond.incrementAndGet();
                            OPERATION_COUNTERS.get(prefix).get("delete").incrementAndGet();
                        }
                        break;
                }
            });

            // Kiểm tra xem tất cả prefix và loại thao tác đã đạt giới hạn chưa
            allOperationsReachedLimit = true;
            for (Map.Entry<String, Map<String, AtomicInteger>> prefixEntry : OPERATION_COUNTERS.entrySet()) {
                String prefix = prefixEntry.getKey();
                Map<String, AtomicInteger> operations = prefixEntry.getValue();
                for (AtomicInteger counter : operations.values()) {
                    if (counter.get() < PREFIX_LIMITS.get(prefix)) {
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

        scheduler.shutdown();
        System.out.println("\nTong so thao tac:");
        System.out.println("Them moi: " + totalInsertCount.get());
        System.out.println("Cap nhat: " + totalUpdateCount.get());
        System.out.println("Xoa: " + totalDeleteCount.get());
        System.out.println("\nDa hoan thanh so luong thay doi cho tat ca prefix:");
        for (Map.Entry<String, Map<String, AtomicInteger>> prefixEntry : OPERATION_COUNTERS.entrySet()) {
            String prefix = prefixEntry.getKey();
            Map<String, AtomicInteger> operations = prefixEntry.getValue();
            System.out.printf("  %s:\n", prefix);
            System.out.printf("    Insert: %d/%d\n", 
                operations.get("insert").get(), PREFIX_LIMITS.get(prefix));
            System.out.printf("    Update: %d/%d\n", 
                operations.get("update").get(), PREFIX_LIMITS.get(prefix));
            System.out.printf("    Delete: %d/%d\n", 
                operations.get("delete").get(), PREFIX_LIMITS.get(prefix));
        }

        client.close();
    }

    private static boolean performInsert(AerospikeClient client, WritePolicy writePolicy, String namespace, String setName,
            Random random, String prefix) {
        // Kiểm tra giới hạn cho prefix và loại thao tác
        if (OPERATION_COUNTERS.get(prefix).get("insert").get() >= PREFIX_LIMITS.get(prefix)) {
            return false;
        }

        int number = random.nextInt(MAX_RECORDS_PER_PREFIX) + 1;
        String phoneNumber = String.format("%s%07d", prefix, number);
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

        byte[] personBytes = generateRandomBytes(random, 100, 1_000);
        Bin personBin = new Bin("personData", personBytes);
        Bin lastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis());

        try {
            client.put(writePolicy, key, personBin, lastUpdateBin);
            return true;
        } catch (AerospikeException e) {
            System.err.println("Loi khi them ban ghi voi key: " + key.userKey + " (loi: " + e.getMessage() + ")");
            return false;
        }
    }

    private static boolean performUpdate(AerospikeClient client, WritePolicy writePolicy, Policy readPolicy,
            String namespace, String setName, ConcurrentLinkedQueue<Key> randomKeys, Random random, String prefix) {
        // Kiểm tra giới hạn cho prefix và loại thao tác
        if (OPERATION_COUNTERS.get(prefix).get("update").get() >= PREFIX_LIMITS.get(prefix)) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de cap nhat.");
            return false;
        }

        try {
            // Lấy prefix từ key
            String keyStr = new String((byte[])randomKey.userKey.getObject());
            String keyPrefix = keyStr.substring(0, 3);
            
            // Chỉ cập nhật nếu prefix khớp
            if (!keyPrefix.equals(prefix)) {
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

            // Kiểm tra xem bản ghi đã bị xóa chưa (personData là null)
            if (record.getValue("personData") == null) {
                // Bản ghi đã bị xóa, không thực hiện update
                randomKeys.offer(randomKey);
                return false;
            }

            byte[] updatedBytes = generateRandomBytes(random, 100, 1_000);
            Bin updatedPersonBin = new Bin("personData", updatedBytes);
            Bin updatedLastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis());

            client.put(writePolicy, randomKey, updatedPersonBin, updatedLastUpdateBin);
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
            ConcurrentLinkedQueue<Key> randomKeys, String prefix) {
        // Kiểm tra giới hạn cho prefix và loại thao tác
        if (OPERATION_COUNTERS.get(prefix).get("delete").get() >= PREFIX_LIMITS.get(prefix)) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("Khong tim thay ban ghi de xoa.");
            return false;
        }

        try {
            // Lấy prefix từ key
            String keyStr = new String((byte[])randomKey.userKey.getObject());
            String keyPrefix = keyStr.substring(0, 3);
            
            // Chỉ xóa nếu prefix khớp
            if (!keyPrefix.equals(prefix)) {
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

            // Kiểm tra xem bản ghi đã bị xóa chưa (personData là null)
            if (record.getValue("personData") == null) {
                // Bản ghi đã bị xóa, không thực hiện delete
                randomKeys.offer(randomKey);
                return false;
            }

            Bin deleteBin = Bin.asNull("personData");
            Bin lastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis());
            client.put(null, randomKey, deleteBin, lastUpdateBin);
            return true;
        } catch (AerospikeException e) {
            System.err.println(
                    "Loi khi xoa ban ghi voi key: " + randomKey.userKey + " (loi: " + e.getMessage() + ")");
        }
        return false;
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
