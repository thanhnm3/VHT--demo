package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;

import com.example.pipeline.AProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainProducer {
    private static final Logger logger = LoggerFactory.getLogger(MainProducer.class);

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Get Kafka configuration
            String kafkaBroker = config.getKafka().getBroker();

            // Delete and recreate topics before starting
            logger.info("Deleting all topics from Kafka...");
            DeleteTopic.deleteAllTopics(kafkaBroker);

            // Performance configuration
            int producerThreadPoolSize = config.getPerformance().getWorker_pool().getProducer();
            int maxRetries = config.getPerformance().getMax_retries();

            // Create thread pool for Producer
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> producerLatches = new ArrayList<>();

            logger.info("=== Starting Producer Only ===");
            logger.info("Kafka Broker: {}", kafkaBroker);
            logger.info("Producer Thread Pool Size: {}", producerThreadPoolSize);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Initialize producer
            Config.Producer producer = config.getProducers().get(0);
            CountDownLatch producerDone = new CountDownLatch(1);
            producerLatches.add(producerDone);

            // Create topics and consumer groups for each region
            Map<String, String> regionTopics = new HashMap<>();
            Map<String, String> regionToConsumerGroup = new HashMap<>();
            
            for (String region : config.getRegion_mapping().keySet()) {
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), region);
                String producerTopic = TopicGenerator.generateATopicName(baseTopic);
                regionTopics.put(region, producerTopic);
                
                // Generate consumer group for this region
                String consumerGroup = generateConsumerGroup(producer.getName(), region);
                regionToConsumerGroup.put(region, consumerGroup);
            }

            // Collect all consumer groups for producer
            String allConsumerGroups = String.join(",", regionToConsumerGroup.values());

            logger.info("[PRODUCER] Starting with configuration:");
            logger.info("[PRODUCER] - Host: {}", producer.getHost());
            logger.info("[PRODUCER] - Port: {}", producer.getPort());
            logger.info("[PRODUCER] - Namespace: {}", producer.getNamespace());
            logger.info("[PRODUCER] - Set: {}", producer.getSet());
            logger.info("[PRODUCER] - Region Topics: {}", regionTopics);
            logger.info("[PRODUCER] - Consumer Groups: {}", allConsumerGroups);

            // Start Producer
            executor.submit(() -> {
                try {
                    String[] producerArgs = new String[] {
                        kafkaBroker,              // kafkaBroker
                        producer.getHost(),          // aerospikeHost
                        String.valueOf(producer.getPort()), // aerospikePort
                        producer.getNamespace(),     // namespace
                        producer.getSet(),           // setName
                        String.valueOf(maxRetries),  // maxRetries
                        String.join(",", regionTopics.values()), // topics (comma-separated list)
                        String.valueOf(producerThreadPoolSize), // workerPoolSize
                        allConsumerGroups // consumerGroups (comma-separated list)
                    };
                    
                    AProducer.main(producerArgs);
                } catch (Exception e) {
                    logger.error("[PRODUCER] Failed: {}", e.getMessage(), e);
                } finally {
                    producerDone.countDown();
                }
            });

            // Add shutdown hook to handle program termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("[MAIN] Shutting down producer...");
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

            // Wait for producer to finish
            try {
                for (CountDownLatch latch : producerLatches) {
                    latch.await();
                }
                executor.shutdown();
                logger.info("[MAIN] Producer completed successfully.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("[MAIN] Producer interrupted.");
            }
        } catch (Exception e) {
            logger.error("Serious error: {}", e.getMessage(), e);
        }
    }

    // Common method to generate consumer group
    private static String generateConsumerGroup(String producerName, String region) {
        return TopicGenerator.generateAGroupName(producerName + "_" + region);
    }
} 