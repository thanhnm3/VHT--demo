package com.delete;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.Vertx;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteAerospikeSet {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        AerospikeClient client = new AerospikeClient("localhost", 4000);
        String namespace = "consumer";
        String setName = "users";

        ScanPolicy scanPolicy = new ScanPolicy();
        WritePolicy writePolicy = new WritePolicy();
        AtomicInteger deletedCount = new AtomicInteger(0); // Đếm số bản ghi đã xóa

        try {
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                try {
                    client.delete(writePolicy, key);
                    deletedCount.incrementAndGet(); // Tăng số lượng bản ghi đã xóa
                } catch (Exception e) {
                    System.err.println("Lỗi khi xóa bản ghi: " + e.getMessage());
                }
            });
        } finally {
            System.out.println("Tổng số bản ghi đã xóa: " + deletedCount.get());
            client.close();
            vertx.close();
        }
    }
}
