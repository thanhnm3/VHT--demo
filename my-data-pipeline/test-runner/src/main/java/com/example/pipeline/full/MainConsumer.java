package com.example.pipeline.full;

import com.example.pipeline.AConsumer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import java.util.List;
import java.util.Map;

public class MainConsumer {
    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Get prefix mapping
            Map<String, List<String>> prefixMapping = config.getPrefix_mapping();
            
            // Get Kafka broker from config
            String kafkaBrokerTarget = config.getKafka().getBrokers().getTarget();
            
            // Default values for performance
            int consumerThreadPoolSize = 4;
            int maxMessagesPerSecond = config.getPerformance().getMax_messages_per_second();

            // Process each prefix and its consumers
            for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();
                
                if (consumerNames.isEmpty()) {
                    System.err.println("Warning: No consumers found for prefix " + prefix);
                    continue;
                }

                // Get the first consumer for this prefix
                String consumerName = consumerNames.get(0);
                Config.Consumer consumer = config.getConsumers().stream()
                    .filter(c -> c.getName().equals(consumerName))
                    .findFirst()
                    .orElse(null);

                if (consumer == null) {
                    System.err.println("Warning: No consumer config found for " + consumerName);
                    continue;
                }

                System.out.println("=== Starting Consumer for prefix " + prefix + " ===");
                System.out.println("Consumer Name: " + consumerName);
                System.out.println("Kafka Broker: " + kafkaBrokerTarget);
                System.out.println("Source Host: " + consumer.getHost());
                System.out.println("Source Port: " + consumer.getPort());
                System.out.println("Source Namespace: " + consumer.getNamespace());
                System.out.println("Destination Host: " + consumer.getHost());
                System.out.println("Destination Port: " + consumer.getPort());
                System.out.println("Worker Pool Size: " + consumerThreadPoolSize);
                System.out.println("Max Messages Per Second: " + maxMessagesPerSecond);
                System.out.println("===========================");

                // Chạy Consumer
                AConsumer.main(args, consumerThreadPoolSize, maxMessagesPerSecond,
                        consumer.getHost(), consumer.getPort(), consumer.getNamespace(),
                        consumer.getHost(), consumer.getPort(), 
                        kafkaBrokerTarget);
            }

        } catch (Exception e) {
            System.err.println("Error in Consumer: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 