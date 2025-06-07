package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.Record;
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

            // Configure client với cấu hình đơn giản
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.timeout = 30000;      // Tăng timeout lên 30 giây
            clientPolicy.maxConnsPerNode = 1;  // Giảm số kết nối
            clientPolicy.tendInterval = 0;     // Tắt tend thread
            clientPolicy.rackAware = false;    // Tắt rack awareness

            // Chỉ kết nối đến một node
            Host[] hosts = new Host[] {
                new Host("localhost", 3000)
            };
            
            System.out.println("Đang kết nối đến Aerospike...");
            AerospikeClient client = new AerospikeClient(clientPolicy, hosts);
            System.out.println("Kết nối thành công!");

            // Create write policy
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;

            // Generate a unique key
            String key = "test_" + System.currentTimeMillis();
            
            // Create bins
            Bin messageBin = new Bin("message", "Hello from cluster");
            Bin timestampBin = new Bin("timestamp", System.currentTimeMillis());

            // Insert record
            Key recordKey = new Key(producerNamespace, producerSetName, key.getBytes());
            client.put(policy, recordKey, messageBin, timestampBin);
            
            System.out.println("Đã insert thành công record với key: " + key);

            // Đọc record để kiểm tra
            Record record = client.get(policy, recordKey);
            System.out.println("Đọc record:");
            System.out.println("Message: " + record.getString("message"));
            System.out.println("Timestamp: " + record.getLong("timestamp"));

            // Close client
            client.close();
            System.out.println("Đã đóng kết nối Aerospike");

        } catch (Exception e) {
            System.err.println("Lỗi: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
