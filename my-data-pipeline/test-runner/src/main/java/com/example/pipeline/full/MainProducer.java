package com.example.pipeline.full;

import com.example.pipeline.AProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

public class MainProducer {
    public static void main(String[] args) {
        try {
            // Load configuration from Config
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Lấy cấu hình từ Config
            String sourceHost = config.getProducers().get(0).getHost();
            int sourcePort = config.getProducers().get(0).getPort();
            String sourceNamespace = config.getProducers().get(0).getNamespace();
            String producerSetName = config.getProducers().get(0).getSet();
            String kafkaBrokerSource = config.getKafka().getBrokers().getSource();
            int producerThreadPoolSize = 2; // Số thread cho Producer
            int maxMessagesPerSecond = config.getPerformance().getMax_messages_per_second();
            int maxRetries = config.getPerformance().getMax_retries();
            String consumerGroup = config.getConsumers().get(0).getName() + "-group";

            System.out.println("=== Starting Producer Only ===");
            System.out.println("Kafka Broker: " + kafkaBrokerSource);
            System.out.println("Source Host: " + sourceHost);
            System.out.println("Source Port: " + sourcePort);
            System.out.println("Source Namespace: " + sourceNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("Worker Pool Size: " + producerThreadPoolSize);
            System.out.println("Max Messages Per Second: " + maxMessagesPerSecond);
            System.out.println("Max Retries: " + maxRetries);
            System.out.println("Consumer Group: " + consumerGroup);
            System.out.println("===========================");

            // Chạy Producer
            AProducer.main(args, producerThreadPoolSize, maxMessagesPerSecond,
                    sourceHost, sourcePort, sourceNamespace, producerSetName,
                    kafkaBrokerSource, maxRetries, consumerGroup);

        } catch (Exception e) {
            System.err.println("Error in Producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 