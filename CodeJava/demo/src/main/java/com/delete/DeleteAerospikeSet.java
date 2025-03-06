package com.delete;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ScanPolicy;

public class DeleteAerospikeSet {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        String namespace = "sub";
        String setName = "users";

        ScanPolicy scanPolicy = new ScanPolicy();
        WritePolicy writePolicy = new WritePolicy();

        try {
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                client.delete(writePolicy, key);
                System.out.println("Đã xóa bản ghi: " + key);
            });
        } finally {
            client.close();
        }
    }
}
