package com.insert;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomInsert {
    public static void main(String[] args) {
        //  Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        WritePolicy policy = new WritePolicy();
        policy.sendKey = true;

        String namespace = "producer";
        String setName = "users";
        Random random = new Random();

        int numThreads = 8; // Số lượng luồng song song
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        long startTime = System.currentTimeMillis();
        int duration = 10_000; // Chạy trong 10 giây
        int batchSize = 500; // Kích thước batch

        AtomicInteger totalCount = new AtomicInteger(0); // Tổng số bản ghi đã insert
        AtomicInteger lastSecondCount = new AtomicInteger(0); // Đếm số bản ghi mỗi giây

        // 🛠 Luồng in TPS mỗi giây
        ScheduledExecutorService tpsLogger = Executors.newScheduledThreadPool(1);
        tpsLogger.scheduleAtFixedRate(() -> {
            int count = lastSecondCount.getAndSet(0);
            System.out.println(" Toc do ghi: " + count + " records/sec");
        }, 1, 1, TimeUnit.SECONDS); // Cập nhật mỗi giây

        // 🛠 Luồng thực hiện ghi dữ liệu
        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                while (System.currentTimeMillis() - startTime < duration) {
                    List<Key> keys = new ArrayList<>();
                    List<Bin[]> binsList = new ArrayList<>();
                    for (int j = 0; j < batchSize; j++) {
                        // 🟢 Tạo dữ liệu ngẫu nhiên với kích thước từ 100B đến 1KB
                        byte[] personBytes = generateRandomBytes(random, 100, 1_000);
                        
                        // 🟢 Sinh UUID
                        String userId = UUID.randomUUID().toString();
                        Key key = new Key(namespace, setName, userId);
                        Bin personBin = new Bin("personData", personBytes);

                        // 🟢 Thay thế last_update bằng migrated_gen với giá trị mặc định là null
                        Bin migratedGenBin = new Bin("migrated_gen", 0);

                        // 🟢 Thêm vào batch
                        keys.add(key);
                        binsList.add(new Bin[]{personBin, migratedGenBin});
                    }

                    // 🟢 Ghi batch vào Aerospike
                    for (int k = 0; k < keys.size(); k++) {
                        client.put(policy, keys.get(k), binsList.get(k));
                    }
                    totalCount.addAndGet(batchSize);
                    lastSecondCount.addAndGet(batchSize);
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(duration + 5000, TimeUnit.MILLISECONDS); // Đợi tất cả các luồng hoàn thành
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 🛠 Kết thúc đo TPS
        tpsLogger.shutdown();

        System.out.println("\n Tong so ban ghi: " + totalCount.get());

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

