package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import com.example.pipeline.AConsumer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MainConsumer.class);

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Get Kafka configuration
            String kafkaBrokerTarget = config.getKafka().getBroker();

            // Performance configuration
            int consumerThreadPoolSize = config.getPerformance().getWorker_pool().getConsumer();
            int maxRetries = config.getPerformance().getMax_retries();

            // Create thread pool for Consumer
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> consumerLatches = new ArrayList<>();

            logger.info("=== Starting A Consumers Only ===");
            logger.info("Kafka Broker Target: {}", kafkaBrokerTarget);
            logger.info("Consumer Thread Pool Size: {}", consumerThreadPoolSize);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Start Consumers by region
            Map<String, List<String>> regionMapping = config.getRegion_mapping();
            for (Map.Entry<String, List<String>> entry : regionMapping.entrySet()) {
                String region = entry.getKey();
                List<String> consumerNames = entry.getValue();

                if (consumerNames.isEmpty()) {
                    logger.warn("No consumers found for region: {}", region);
                    continue;
                }

                // Create topic and consumer group for this region
                String producerName = config.getProducers().get(0).getName();
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producerName, region);
                String consumerTopic = TopicGenerator.generateATopicName(baseTopic);
                final String consumerGroup = TopicGenerator.generateAGroupName(producerName + "_" + region);

                logger.info("Starting Consumers for region {}: {}", region, consumerNames);
                logger.info("  Base Topic: {}", baseTopic);
                logger.info("  Consumer Topic: {}", consumerTopic);
                logger.info("  Consumer Group: {}", consumerGroup);

                // Start Consumers for this region
                for (String consumerName : consumerNames) {
                    Config.Consumer consumer = config.getConsumers().stream()
                        .filter(c -> c.getName().equals(consumerName))
                        .findFirst()
                        .orElse(null);

                    if (consumer == null) {
                        logger.warn("No consumer config found for: {}", consumerName);
                        continue;
                    }

                    CountDownLatch consumerDone = new CountDownLatch(1);
                    consumerLatches.add(consumerDone);

                    logger.info("Starting A Consumer for region {}: {}", region, consumerName);
                    logger.info("  Host: {}", consumer.getHost());
                    logger.info("  Port: {}", consumer.getPort());
                    logger.info("  Namespace: {}", consumer.getNamespace());
                    logger.info("  Set: {}", consumer.getSet());
                    logger.info("  Base Topic: {}", baseTopic);
                    logger.info("  Consumer Topic: {}", consumerTopic);
                    logger.info("  Consumer Group: {}", consumerGroup);

                    // Start Consumer
                    executor.submit(() -> {
                        try {
                            String[] consumerArgs = new String[] {
                                kafkaBrokerTarget,           // kafkaBroker
                                consumerTopic,              // consumerTopic
                                consumerGroup,              // consumerGroup
                                consumer.getHost(),         // aerospikeHost
                                String.valueOf(consumer.getPort()), // aerospikePort
                                consumer.getNamespace(),    // aerospikeNamespace
                                consumer.getSet(),          // aerospikeSetName
                                String.valueOf(consumerThreadPoolSize) // workerPoolSize
                            };
                            
                            AConsumer.main(consumerArgs);
                        } catch (Exception e) {
                            logger.error("Error in Consumer {}: {}", consumerName, e.getMessage(), e);
                        } finally {
                            consumerDone.countDown();
                        }
                    });
                }
            }

            // Add shutdown hook to handle program termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down program...");
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }));

            // Wait for all consumers to finish
            try {
                for (CountDownLatch latch : consumerLatches) {
                    latch.await();
                }
                executor.shutdown();
                logger.info("Program has ended.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Program was interrupted.");
            }
        } catch (Exception e) {
            logger.error("Serious error: {}", e.getMessage(), e);
        }
    }
} 