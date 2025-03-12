package com.delete;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ScanPolicy;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteAerospikeSet {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        String namespace = "sub";
        String setName = "users";

        ScanPolicy scanPolicy = new ScanPolicy();
        WritePolicy writePolicy = new WritePolicy();

        // Create a thread pool with a fixed number of threads
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        AtomicInteger count = new AtomicInteger(0);

        try {
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                executor.submit(() -> {
                    client.delete(writePolicy, key);
                    System.out.println("Đã xóa bản ghi: " + count.incrementAndGet());
                });
            });
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
            client.close();
        }
    }
}
