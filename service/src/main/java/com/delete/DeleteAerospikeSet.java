package com.delete;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.Vertx;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteAerospikeSet {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        AerospikeClient client = new AerospikeClient("localhost", 4000);
        String namespace = "consumer";
        String setName = "users";

        ScanPolicy scanPolicy = new ScanPolicy();
        WritePolicy writePolicy = new WritePolicy();

        AtomicInteger count = new AtomicInteger(0);

        // Tạo một ExecutorService với số luồng bằng số lõi CPU
        int threadPoolSize = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

        try {
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                // Gửi tác vụ xóa vào ExecutorService
                executor.submit(() -> {
                    try {
                        client.delete(writePolicy, key);
                        System.out.println("Đã xóa bản ghi: " + count.incrementAndGet());
                    } catch (Exception e) {
                        System.err.println("Lỗi khi xóa bản ghi: " + key.userKey + " - " + e.getMessage());
                    }
                });
            });
        } finally {
            // Đóng ExecutorService sau khi hoàn thành
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }

            client.close();
            vertx.close();
        }
    }
}
