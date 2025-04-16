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

public class RandomOperations {
    public static void main(String[] args) {
        // Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        WritePolicy writePolicy = new WritePolicy();
        Policy readPolicy = new Policy(); // Sử dụng readPolicy
        writePolicy.sendKey = true;

        String namespace = "producer";
        String setName = "users";
        Random random = new Random();

        int operationsPerSecond = 1; // Số lượng thao tác mỗi giây (có thể điều chỉnh)
        RateLimiter rateLimiter = RateLimiter.create(operationsPerSecond); // Tạo RateLimiter để kiểm soát tốc độ

        AtomicInteger totalInsertCount = new AtomicInteger(0);
        AtomicInteger totalUpdateCount = new AtomicInteger(0);
        AtomicInteger totalDeleteCount = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();
        long duration = 100_000; // Chạy trong 100 giây

        while (System.currentTimeMillis() - startTime < duration) {
            rateLimiter.acquire(); // Đảm bảo chỉ thực hiện số thao tác tối đa mỗi giây

            int operationType = random.nextInt(3); // 0: Insert, 1: Update, 2: Delete
            switch (operationType) {
                case 0: // Insert
                    performInsert(client, writePolicy, namespace, setName, random);
                    totalInsertCount.incrementAndGet();
                    break;
                case 1: // Update
                    performUpdate(client, writePolicy, readPolicy, namespace, setName, random);
                    totalUpdateCount.incrementAndGet();
                    break;
                case 2: // Delete
                    performDelete(client, namespace, setName, random);
                    totalDeleteCount.incrementAndGet();
                    break;
            }
        }

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
        System.out.println("Inserted record with key: " + userId);
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
                System.out.println("Updated record with key: " + randomKey.userKey);
            }
        } catch (AerospikeException e) {
            System.err.println("Failed to update record with key: " + randomKey.userKey);
        }
    }

    private static void performDelete(AerospikeClient client, String namespace, String setName, Random random) {
        String userId = UUID.randomUUID().toString(); // Giả định xóa một key ngẫu nhiên
        Key key = new Key(namespace, setName, userId);

        try {
            boolean deleted = client.delete(null, key);
            if (deleted) {
                System.out.println("Deleted record with key: " + userId);
            }
        } catch (AerospikeException e) {
            System.err.println("Failed to delete record with key: " + userId);
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
