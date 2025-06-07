package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

public class Insert {
    public static void main(String[] args) {
        try {
            // Load configuration
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Không thể load cấu hình");
            }

            // Get producer config
            String producerHost = config.getProducers().get(0).getHost();
            int producerPort = config.getProducers().get(0).getPort();
            String producerNamespace = config.getProducers().get(0).getNamespace();
            String producerSetName = config.getProducers().get(0).getSet();

            System.out.println("=== Bắt đầu Insert ===");
            System.out.println("Producer Host: " + producerHost);
            System.out.println("Producer Port: " + producerPort);
            System.out.println("Producer Namespace: " + producerNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("===============================");

            // Configure client
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.tendInterval = 0;     // Set to 0 to disable tend thread
            clientPolicy.maxConnsPerNode = 1;  // Minimize connections
            clientPolicy.timeout = 10000;      // Increase timeout
            clientPolicy.rackAware = false;
            clientPolicy.rackId = 0;

            // Connect to Aerospike through port mapping
            Host[] hosts = new Host[] {
                new Host("localhost", 3000)  // Use localhost with port mapping
            };
            
            System.out.println("Đang kết nối qua port mapping: localhost:3000");
            AerospikeClient client = new AerospikeClient(clientPolicy, hosts);
            System.out.println("Kết nối đến Aerospike thành công!");

            // Create write policy
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;

            // Generate a unique key
            String key = "test_" + System.currentTimeMillis();
            
            // Create bins
            Bin messageBin = new Bin("message", "Hello");
            Bin timestampBin = new Bin("timestamp", System.currentTimeMillis());

            // Insert record
            Key recordKey = new Key(producerNamespace, producerSetName, key.getBytes());
            client.put(policy, recordKey, messageBin, timestampBin);
            
            System.out.println("Đã insert thành công record với key: " + key);
            System.out.println("Message: Hello");
            System.out.println("Timestamp: " + System.currentTimeMillis());

            // Close client
            client.close();
            System.out.println("Đã đóng kết nối Aerospike");

        } catch (Exception e) {
            System.err.println("Lỗi: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
