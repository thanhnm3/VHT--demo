package com.delete;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.Vertx;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DeleteAerospikeSet {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        AerospikeClient client = new AerospikeClient("localhost", 4000);
        String namespace = "consumer";
        String setName = "users";

        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true; // Cho phép quét song song trên nhiều node
        scanPolicy.includeBinData = false; // Không cần tải dữ liệu bin, chỉ cần key

        WritePolicy writePolicy = new WritePolicy();
        AtomicInteger deletedCount = new AtomicInteger(0); // Đếm số bản ghi đã xóa

        ExecutorService executor = Executors.newFixedThreadPool(10); // Sử dụng executor để xử lý song song

        try {
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                executor.submit(() -> { // Sử dụng executor để xử lý song song
                    try {
                        client.delete(writePolicy, key);
                        deletedCount.incrementAndGet(); // Tăng số lượng bản ghi đã xóa
                        System.out.println("Đã xóa bản ghi: " + deletedCount.get());
                    } catch (Exception e) {
                        System.err.println("Lỗi khi xóa bản ghi: " + e.getMessage());
                    }
                });
            });
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS); // Chờ tất cả các luồng hoàn thành
            System.out.println("Tổng số bản ghi đã xóa: " + deletedCount.get());
        } catch (InterruptedException e) {
            System.err.println("Lỗi khi chờ các luồng hoàn thành: " + e.getMessage());
        } finally {
            client.close();
            vertx.close();
        }
    }
}
