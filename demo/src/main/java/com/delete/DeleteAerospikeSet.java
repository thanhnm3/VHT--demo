package com.delete;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.Vertx;

import java.util.concurrent.atomic.AtomicInteger;

public class DeleteAerospikeSet {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        String namespace = "sub";
        String setName = "users";

        ScanPolicy scanPolicy = new ScanPolicy();
        WritePolicy writePolicy = new WritePolicy();

        AtomicInteger count = new AtomicInteger(0);

        try {
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                client.delete(writePolicy, key);
                System.out.println("Đã xóa bản ghi: " + count.incrementAndGet());
            });
        } finally {
            client.close();
            vertx.close();
        }
    }
}
