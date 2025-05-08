package com.example.pipeline.cdc;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import io.github.cdimascio.dotenv.Dotenv;

public class RandomInsert {
    private static final String[] PHONE_PREFIXES = {
        "096", "033"
    };

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure()
                .directory("my-data-pipeline/.env")
                .load();
        // Kết nối đến Aerospike
        String aerospikeHost = dotenv.get("AEROSPIKE_PRODUCER_HOST");
        int aerospikePort = Integer.parseInt(dotenv.get("AEROSPIKE_PRODUCER_PORT"));
        String namespace = dotenv.get("PRODUCER_NAMESPACE");
        String setName = dotenv.get("PRODUCER_SET_NAME");
        AerospikeClient client = new AerospikeClient(aerospikeHost, aerospikePort);
        System.out.println("Kết nối đến Aerospike thành công!");
        WritePolicy policy = new WritePolicy();
        policy.sendKey = true;


        int numThreads = 2; // Số lượng luồng song song (mỗi luồng xử lý 1 prefix)
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        int maxRecordsPerPrefix = 200_000; // 200 000 bản ghi cho mỗi prefix
        AtomicInteger totalCount = new AtomicInteger(0); // Tổng số bản ghi đã insert

        // 🛠 Luồng thực hiện ghi dữ liệu
        for (String prefix : PHONE_PREFIXES) {
            executor.submit(() -> {
                for (int i = 1; i <= maxRecordsPerPrefix; i++) {
                    String phoneNumber = String.format("%s%07d", prefix, i); // Tạo số điện thoại tăng dần
                    byte[] phoneBytes = phoneNumber.getBytes();

                    // 🟢 Tạo dữ liệu ngẫu nhiên với kích thước từ 100B đến 1KB
                    byte[] personBytes = generateRandomBytes(100, 1_000);

                    Key key = new Key(namespace, setName, phoneBytes);
                    Bin personBin = new Bin("personData", personBytes);

                    // 🟢 Giữ lastUpdate ở dạng timestamp
                    Bin lastUpdateBin = new Bin("lastUpdate", System.currentTimeMillis());

                    // 🟢 Ghi vào Aerospike
                    client.put(policy, key, personBin, lastUpdateBin);

                    // Cập nhật bộ đếm
                    totalCount.incrementAndGet();

                    // In tiến trình mỗi 100.000 bản ghi
                    if (i % 100_000 == 0) {
                        System.out.printf("Prefix %s: Đã ghi %d/%d bản ghi%n", prefix, i, maxRecordsPerPrefix);
                    }
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS); // Đợi tất cả các luồng hoàn thành
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("\nTổng số bản ghi đã ghi: " + totalCount.get());

        // Đóng kết nối
        client.close();
    }

    // Phương thức để tạo byte array với kích thước ngẫu nhiên từ minSize đến maxSize bytes
    private static byte[] generateRandomBytes(int minSize, int maxSize) {
        int size = ThreadLocalRandom.current().nextInt(minSize, maxSize + 1);
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
