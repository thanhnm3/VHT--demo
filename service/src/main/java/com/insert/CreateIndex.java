package com.insert;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.query.IndexType;

public class CreateIndex {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        try {
            String namespace = "producer";
            String set = "users";
            String indexName = "idx_last_update";

            Policy policy = new Policy();
            IndexTask task = client.createIndex(policy, namespace, set, indexName, "last_update", IndexType.NUMERIC);

            task.waitTillComplete(); // Đợi tạo xong
            System.out.println("Secondary Index created.");
        } catch (AerospikeException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
