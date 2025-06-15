package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

public class AerospikeService {
    private final AerospikeClient client;
    private final WritePolicy writePolicy;

    public AerospikeService(String host, int port) {
        this(host, port, 300); // Default max connections per node
    }

    public AerospikeService(String host, int port, int maxConnsPerNode) {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = maxConnsPerNode;
        this.client = new AerospikeClient(clientPolicy, host, port);
        this.writePolicy = createWritePolicy();
    }

    private WritePolicy createWritePolicy() {
        WritePolicy policy = new WritePolicy();
        policy.totalTimeout = 5000; // 5 seconds timeout
        policy.sendKey = true;
        return policy;
    }

    public AerospikeClient getClient() {
        return client;
    }

    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    public void shutdown() {
        if (client != null) {
            client.close();
        }
    }
} 