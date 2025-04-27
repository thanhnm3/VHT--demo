package com.cdc;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.google.common.util.concurrent.RateLimiter;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

// Can cai thien lai code rat nhieu, nhung ma co ban thi van chay tot
public class RandomOperations {
    public static void main(String aeroHost, int aeroPort, String namespace, String setName, int operationsPerSecond,
            int threadPoolSize) {
        // Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient(aeroHost, aeroPort);
        WritePolicy writePolicy = new WritePolicy();
        Policy readPolicy = new Policy(); // Sử dụng readPolicy
        writePolicy.sendKey = true;

        Random random = new Random();

        // Sử dụng RateLimiter để kiểm soát tốc độ
        RateLimiter rateLimiter = RateLimiter.create(operationsPerSecond);

        AtomicInteger totalInsertCount = new AtomicInteger(0);
        AtomicInteger totalUpdateCount = new AtomicInteger(0);
        AtomicInteger totalDeleteCount = new AtomicInteger(0);

        AtomicInteger insertCountThisSecond = new AtomicInteger(0);
        AtomicInteger updateCountThisSecond = new AtomicInteger(0);
        AtomicInteger deleteCountThisSecond = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        long duration = 20_000; // Chạy trong x giây

        ConcurrentLinkedQueue<Key> randomKeys = getRandomKeysFromDatabase(client, namespace, setName, 10000);

        // Tạo thread pool chỉ cho producer (gửi dữ liệu)
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

        // Scheduler chỉ dùng 1 thread để in log mỗi giây
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("Insert: " + insertCountThisSecond.get() +
                    ", Update: " + updateCountThisSecond.get() +
                    ", Delete: " + deleteCountThisSecond.get());
            insertCountThisSecond.set(0);
            updateCountThisSecond.set(0);
            deleteCountThisSecond.set(0);
        }, 0, 1, TimeUnit.SECONDS);

        while (System.currentTimeMillis() - startTime < duration) {
            rateLimiter.acquire(); // Đảm bảo chỉ thực hiện số thao tác tối đa mỗi giây

            executor.submit(() -> {
                int operationType = random.nextInt(3); // 0: Insert, 1: Update, 2: Delete
                switch (operationType) {
                    case 0: // Insert
                        performInsert(client, writePolicy, namespace, setName, random);
                        totalInsertCount.incrementAndGet();
                        insertCountThisSecond.incrementAndGet();
                        break;
                    case 1: // Update
                        performUpdate(client, writePolicy, readPolicy, namespace, setName, randomKeys, random);
                        totalUpdateCount.incrementAndGet();
                        updateCountThisSecond.incrementAndGet();
                        break;
                    case 2: // Delete
                        performDelete(client, namespace, setName, randomKeys);
                        totalDeleteCount.incrementAndGet();
                        deleteCountThisSecond.incrementAndGet();
                        break;
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            System.err.println("Executor interrupted: " + e.getMessage());
        }

        scheduler.shutdown();
        System.out.println("\nTổng số thao tác:");
        System.out.println("Insert: " + totalInsertCount.get());
        System.out.println("Update: " + totalUpdateCount.get());
        System.out.println("Delete: " + totalDeleteCount.get());

        // Đóng kết nối
        client.close();
    }

    private static void performInsert(AerospikeClient client, WritePolicy writePolicy, String namespace, String setName,
            Random random) {
        // Tạo UUID và chuyển sang dạng byte
        UUID uuid = UUID.randomUUID();
        byte[] uuidBytes = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int b = 0; b < 8; b++) {
            uuidBytes[b] = (byte) (msb >>> (8 * (7 - b)));
            uuidBytes[8 + b] = (byte) (lsb >>> (8 * (7 - b)));
        }

        Key key = new Key(namespace, setName, uuidBytes);
        byte[] personBytes = generateRandomBytes(random, 100, 1_000);
        Bin personBin = new Bin("personData", personBytes);
        Bin lastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis());

        client.put(writePolicy, key, personBin, lastUpdateBin);
    }

    private static void performUpdate(AerospikeClient client, WritePolicy writePolicy, Policy readPolicy,
            String namespace, String setName, ConcurrentLinkedQueue<Key> randomKeys, Random random) {
        Key randomKey = randomKeys.poll(); // Lấy key từ hàng đợi
        if (randomKey == null) {
            System.err.println("No records found for update.");
            return;
        }

        try {
            Record record = client.get(readPolicy, randomKey);
            if (record != null) {
                byte[] updatedBytes = generateRandomBytes(random, 100, 1_000);
                Bin updatedPersonBin = new Bin("personData", updatedBytes);
                Bin updatedLastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis());

                client.put(writePolicy, randomKey, updatedPersonBin, updatedLastUpdateBin);
                // System.out.println("Updated record with key: " + randomKey.userKey);
            }
        } catch (AerospikeException e) {
            System.err.println(
                    "Failed to update record with key: " + randomKey.userKey + " (exception: " + e.getMessage() + ")");
        } finally {
            // Trả lại key vào hàng đợi để tái sử dụng
            randomKeys.offer(randomKey);
        }
    }

    private static void performDelete(AerospikeClient client, String namespace, String setName,
            ConcurrentLinkedQueue<Key> randomKeys) {
        Key randomKey = randomKeys.poll(); // Lấy key từ hàng đợi
        if (randomKey == null) {
            System.err.println("No records found for deletion.");
            return; // Không có bản ghi nào để xóa
        }

        try {
            Bin deleteBin = Bin.asNull("personData");
            Bin lastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis()); // Cập nhật lastUpdate khi xóa
            client.put(null, randomKey, deleteBin, lastUpdateBin);
            // System.out.println("Deleted field with key: " + key.userKey);
        } catch (AerospikeException e) {
            System.err.println(
                    "Failed to delete record with key: " + randomKey.userKey + " (exception: " + e.getMessage() + ")");
        }
    }

    // Phương thức để tạo byte array với kích thước ngẫu nhiên từ minSize đến
    // maxSize bytes
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
            System.err.println("Error while fetching keys: " + e.getMessage());
        }

        return keys;
    }

}
