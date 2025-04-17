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



// Can cai thien lai code rat nhieu, nhung ma co ban thi van chay tot
public class RandomOperations {
    public static void main(String aeroHost, int aeroPort, String namespace, String setName, int operationsPerSecond, int threadPoolSize) {
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
                        performUpdate(client, writePolicy, readPolicy, namespace, setName, random);
                        totalUpdateCount.incrementAndGet();
                        updateCountThisSecond.incrementAndGet();
                        break;
                    case 2: // Delete
                        performDelete(client, namespace, setName, random);
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

    private static void performInsert(AerospikeClient client, WritePolicy writePolicy, String namespace, String setName, Random random) {
        String userId = UUID.randomUUID().toString();
        Key key = new Key(namespace, setName, userId);
        byte[] personBytes = generateRandomBytes(random, 100, 1_000);
        Bin personBin = new Bin("personData", personBytes);
        Bin lastUpdateBin = new Bin("last_update", System.currentTimeMillis());

        client.put(writePolicy, key, personBin, lastUpdateBin);
        // System.out.println("Inserted record with key: " + userId);
    }

    private static void performUpdate(AerospikeClient client, WritePolicy writePolicy, Policy readPolicy, String namespace, String setName, Random random) {
        Key randomKey = getRandomKeyFromDatabase(client, namespace, setName, random);
        if (randomKey == null) {
            System.err.println("No records found for update.");
            return;
        }

        try {
            Record record = client.get(readPolicy, randomKey);
            if (record != null) {
                byte[] updatedBytes = generateRandomBytes(random, 100, 1_000);
                Bin updatedPersonBin = new Bin("personData", updatedBytes);
                Bin updatedLastUpdateBin = new Bin("last_update", System.currentTimeMillis());

                client.put(writePolicy, randomKey, updatedPersonBin, updatedLastUpdateBin);
                // System.out.println("Updated record with key: " + randomKey.userKey);
            }
        } catch (AerospikeException e) {
            System.err.println("Failed to update record with key: " + randomKey.userKey);
        }
    }

    private static void performDelete(AerospikeClient client, String namespace, String setName, Random random) {
        // Lấy một key ngẫu nhiên từ cơ sở dữ liệu
        Key key = getRandomKeyFromDatabase(client, namespace, setName, random);

        if (key == null) {
            System.err.println("No records found for deletion.");
            return; // Không có bản ghi nào để sửa
        }

        try {
            Bin deleteBin = Bin.asNull("personData");
            Bin lastUpdateBin = new Bin("last_update", System.currentTimeMillis()); // Cập nhật last_update khi xóa
            client.put(null, key, deleteBin, lastUpdateBin);
            // System.out.println("Deleted field with key: " + key.userKey);
        } catch (AerospikeException e) {
            System.err.println("Failed to delete with key: " + key.userKey + " (exception: " + e.getMessage() + ")");
        }
    }

    // Phương thức để tạo byte array với kích thước ngẫu nhiên từ minSize đến maxSize bytes
    private static byte[] generateRandomBytes(Random random, int minSize, int maxSize) {
        int size = random.nextInt(maxSize - minSize + 1) + minSize;
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static Key getRandomKeyFromDatabase(AerospikeClient client, String namespace, String setName, Random random) {
        QueryPolicy queryPolicy = new QueryPolicy(); // Sử dụng QueryPolicy thay vì ScanPolicy
        queryPolicy.includeBinData = false; // Chỉ lấy key, không cần dữ liệu bin

        Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(setName);

        try (RecordSet recordSet = client.query(queryPolicy, statement)) {
            if (recordSet.next()) {
                // Trả về key của bản ghi đầu tiên tìm thấy
                return recordSet.getKey();
            } else {
                System.err.println("No records found in the database.");
                return null; // Không có bản ghi nào
            }
        } catch (AerospikeException e) {
            System.err.println("Failed to query database: " + e.getMessage());
            return null;
        }
    }
}
