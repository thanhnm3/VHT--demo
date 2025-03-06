package com.insert;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import example.Simple;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AerospikeInsertProto {
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
                    // 🟢 Tạo dữ liệu Protobuf ngẫu nhiên
                    Simple.Person person = Simple.Person.newBuilder()
                            .setName("User_" + random.nextInt(1000)) // Tên ngẫu nhiên
                            .setAge(random.nextInt(50) + 18) // Tuổi từ 18 đến 67
                            .setEmail("user" + random.nextInt(1000) + "@example.com") // Email ngẫu nhiên
                            .build();

                    // 🟢 Chuyển thành byte để lưu vào Aerospike
                    byte[] personBytes = person.toByteArray();

                    // 🟢 Sinh UUID
                    String userId = UUID.randomUUID().toString();
                    Key key = new Key(namespace, setName, userId);
                    Bin personBin = new Bin("personData", personBytes);

                    // 🟢 Ghi vào Aerospike
                    client.put(policy, key, personBin);
                    totalCount.incrementAndGet(); // Tăng tổng số bản ghi đã insert

                    System.out.println("✅ Đã insert: " + person.getName() + " (Key: " + userId + ")");
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
}
