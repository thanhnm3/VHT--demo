package com.example.pipeline.cdc;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

public class RunRandomOperations {
    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Khong the load cau hinh");
            }

            // Lay cau hinh Producer
            String producerHost = config.getProducers().get(0).getHost();
            int producerPort = config.getProducers().get(0).getPort();
            String producerNamespace = config.getProducers().get(0).getNamespace();
            String producerSetName = config.getProducers().get(0).getSet();

            // Cau hinh performance
            int randomOperationsThreadPoolSize = 4; // So thread cho RandomOperations
            int operationsPerSecond = 2000; // So luong thao tac moi giay cho RandomOperations

            System.out.println("=== Bat dau Random Operations ===");
            System.out.println("Producer Host: " + producerHost);
            System.out.println("Producer Port: " + producerPort);
            System.out.println("Producer Namespace: " + producerNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("Random Operations Thread Pool Size: " + randomOperationsThreadPoolSize);
            System.out.println("Operations Per Second: " + operationsPerSecond);
            System.out.println("===============================");

            // Chay RandomOperations
            RandomOperations.main(producerHost, producerPort, producerNamespace, producerSetName, 
                operationsPerSecond, randomOperationsThreadPoolSize);

        } catch (Exception e) {
            System.err.println("Loi: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 