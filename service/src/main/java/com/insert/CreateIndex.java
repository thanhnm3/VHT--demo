package com.insert;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.task.IndexTask;

public class CreateIndex {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("localhost", 3000);
        try {
            String namespace = "producer";
            String set = "users";
            String indexName = "idx_last_update";

            // Xóa index
            Policy policy = new Policy();
            IndexTask task = client.dropIndex(policy, namespace, set, indexName);
            task.waitTillComplete(); // Đợi xóa xong
            System.out.println("Secondary Index dropped.");
        } catch (AerospikeException e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }
}
