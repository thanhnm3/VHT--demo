package com.example.pipeline;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.example.pipeline.model.ConsumerMetrics;
import com.example.pipeline.service.MessageProcessor;
import com.example.pipeline.service.TopicGenerator;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.ConsumerConfig;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class AConsumer {
    private static volatile boolean isShuttingDown = false;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private static Map<String, String> prefixToTopicMap;
    private static Config config;

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

            // Load configuration
            config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Initialize Aerospike client
            final AerospikeClient destinationClient = new AerospikeClient(destinationHost, destinationPort);
            final WritePolicy writePolicy = new WritePolicy();
            writePolicy.totalTimeout = 5000; // 5 seconds timeout
            writePolicy.sendKey = true;

            // Initialize prefix mapping from config
            prefixToTopicMap = TopicGenerator.generateTopics();
            System.out.println("Initialized prefix mapping: " + prefixToTopicMap);

            // Create consumer metrics for each prefix in config
            Map<String, ConsumerMetrics> metricsMap = new HashMap<>();
            
            // Get consumer configurations from config
            Map<String, ConsumerConfig> consumerConfigs = new HashMap<>();
            for (ConsumerConfig consumerConfig : config.getConsumers()) {
                consumerConfigs.put(consumerConfig.getName(), consumerConfig);
            }

            // Create consumers based on prefix mapping
            for (Map.Entry<String, List<String>> entry : config.getPrefix_mapping().entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();
                
                // Get the first consumer for this prefix
                String consumerName = consumerNames.get(0);
                ConsumerConfig consumerConfig = consumerConfigs.get(consumerName);
                
                if (consumerConfig == null) {
                    System.err.println("Warning: No consumer config found for " + consumerName);
                    continue;
                }

                // Get consumer group, namespace and set from config
                String consumerGroup = consumerConfig.getName() + "-group";
                String consumerNamespace = consumerConfig.getNamespace();
                String consumerSetName = consumerConfig.getSet();

                metricsMap.put(prefix, new ConsumerMetrics(
                    sourceNamespace, kafkaBroker,
                    consumerGroup, consumerNamespace,
                    prefix, consumerSetName, workerPoolSize,
                    prefixToTopicMap
                ));
            }

            System.out.println("Consumers subscribed to topics:");
            for (String prefix : prefixToTopicMap.keySet()) {
                System.out.println("- " + prefixToTopicMap.get(prefix));
            }

            // Start monitoring threads for each consumer
            for (ConsumerMetrics metrics : metricsMap.values()) {
                startMonitoringThread(metrics);
            }
            
            // Start processing threads for each consumer
            for (ConsumerMetrics metrics : metricsMap.values()) {
                startProcessingThread(metrics, destinationClient, writePolicy);
            }

            // Wait for shutdown signal
            shutdownLatch.await();
            
            // Graceful shutdown
            System.out.println("Initiating graceful shutdown...");
            for (ConsumerMetrics metrics : metricsMap.values()) {
                metrics.shutdown();
            }
            
            System.out.println("Shutdown completed successfully.");
        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void startMonitoringThread(ConsumerMetrics metrics) {
        new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && !isShuttingDown && metrics.isRunning()) {
                try {
                    if (metrics.getRateControlService().shouldCheckRateAdjustment()) {
                        double oldRate = metrics.getCurrentRate();
                        metrics.monitorAndAdjustLag();
                        if (oldRate != metrics.getCurrentRate()) {
                            System.out.printf("[%s] Rate adjusted from %.2f to %.2f messages/second%n", 
                                            metrics.getPrefix(), oldRate, metrics.getCurrentRate());
                        }
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, metrics.getPrefix() + "-monitor").start();
    }

    private static void startProcessingThread(ConsumerMetrics metrics,
                                           AerospikeClient destinationClient,
                                           WritePolicy writePolicy) {
        Thread processorThread = new Thread(() -> {
            MessageProcessor.processMessages(metrics, destinationClient, writePolicy);
        });
        processorThread.setName(metrics.getPrefix() + "-processor");
        processorThread.start();
    }
}
