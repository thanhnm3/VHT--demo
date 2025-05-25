package com.example.pipeline;

import com.example.pipeline.model.ConsumerMetrics;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.AerospikeService;
import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.MessageService;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class AConsumer {
    private static volatile boolean isShuttingDown = false;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private static ConfigurationService configService;
    private static AerospikeService aerospikeService;
    private static KafkaConsumerService kafkaService;
    private static Map<String, MessageService> messageServices;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String sourceHost, int sourcePort, String sourceNamespace,
                          String destinationHost, int destinationPort, 
                          String kafkaBroker) {
        try {
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown signal received. Starting graceful shutdown...");
                isShuttingDown = true;
                shutdownLatch.countDown();
            }));

            // Initialize configuration service
            configService = ConfigurationService.getInstance();
            if (configService == null) {
                throw new IllegalStateException("Failed to initialize configuration service");
            }

            // Initialize Aerospike service
            aerospikeService = new AerospikeService(destinationHost, destinationPort);

            // Initialize Kafka service
            kafkaService = new KafkaConsumerService(kafkaBroker, configService);
            kafkaService.initializeConsumers(sourceNamespace, workerPoolSize);

            // Initialize message services
            messageServices = new HashMap<>();
            for (Map.Entry<String, ConsumerMetrics> entry : kafkaService.getMetricsMap().entrySet()) {
                String prefix = entry.getKey();
                ConsumerMetrics metrics = entry.getValue();
                
                MessageService messageService = new MessageService(
                    aerospikeService.getClient(),
                    aerospikeService.getWritePolicy(),
                    metrics.getTargetNamespace(),
                    metrics.getSetName(),
                    metrics
                );
                messageServices.put(prefix, messageService);
                
                // Start message service in a new thread
                new Thread(() -> {
                    try {
                        messageService.start();
                    } catch (Exception e) {
                        System.err.printf("[%s] Error in message service: %s%n", 
                                        prefix, e.getMessage());
                        e.printStackTrace();
                    }
                }, prefix + "-message-service").start();
            }

            // Wait for shutdown signal
            shutdownLatch.await();
            
            // Graceful shutdown
            if (isShuttingDown) {
                System.out.println("Initiating graceful shutdown...");
                for (MessageService service : messageServices.values()) {
                    service.shutdown();
                }
                kafkaService.shutdown();
                aerospikeService.shutdown();
            }
            
            System.out.println("Shutdown completed successfully.");
        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
