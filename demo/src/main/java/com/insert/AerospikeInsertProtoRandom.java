package com.insert;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AerospikeInsertProtoRandom {
    public static void main(String[] args) {
        // Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        WritePolicy policy = new WritePolicy();
        policy.sendKey = true;

        String namespace = "pub";
        String setName = "users";
        Random random = new Random();

        int numThreads = 4; // Số lượng luồng
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        long startTime = System.currentTimeMillis();
        int duration = 10_000; // Chạy trong 10 giây

        AtomicInteger totalCount = new AtomicInteger(0); // Tổng số bản ghi đã insert

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                while (System.currentTimeMillis() - startTime < duration) {
                    // 🟢 Tạo dữ liệu ngẫu nhiên với kích thước từ 1 byte đến 100 byte
                    byte[] personBytes = generateRandomBytes(random, 1, 100);
                    
                    // 🟢 Sinh UUID
                    String userId = UUID.randomUUID().toString();
                    Key key = new Key(namespace, setName, userId);
                    Bin personBin = new Bin("personData", personBytes);

                    // 🟢 Ghi vào Aerospike
                    client.put(policy, key, personBin);
                    totalCount.incrementAndGet(); // Tăng tổng số bản ghi đã insert

                    System.out.println("✅ Đã insert: (Key: " + userId + ")");
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(duration + 5000, TimeUnit.MILLISECONDS); // Đợi tất cả các luồng hoàn thành
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("\n🎯 Tổng số bản ghi đã insert: " + totalCount.get()); // In tổng số bản ghi đã insert

        // Đóng kết nối
        client.close();
    }

    // Phương thức để tạo byte array với kích thước ngẫu nhiên từ minSize đến maxSize bytes
    private static byte[] generateRandomBytes(Random random, int minSize, int maxSize) {
        int size = random.nextInt(maxSize - minSize + 1) + minSize;
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }
}
