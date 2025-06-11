package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;

import com.example.pipeline.AConsumer;
import com.example.pipeline.AProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainAll {
    private static final Logger logger = LoggerFactory.getLogger(MainAll.class);

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Get Kafka configuration
            String kafkaBrokerSource = config.getKafka().getBroker();
            String kafkaBrokerTarget = config.getKafka().getBroker();

            // Delete and recreate topics before starting
            logger.info("Deleting all topics from Kafka...");
            DeleteTopic.deleteAllTopics(kafkaBrokerSource);
            DeleteTopic.deleteAllTopics(kafkaBrokerTarget);

            // Performance configuration
            int producerThreadPoolSize = config.getPerformance().getWorker_pool().getProducer();
            int consumerThreadPoolSize = config.getPerformance().getWorker_pool().getConsumer();
            int maxRetries = config.getPerformance().getMax_retries();

            // Create thread pool for Producer and Consumer
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> producerLatches = new ArrayList<>();
            List<CountDownLatch> consumerLatches = new ArrayList<>();

            logger.info("=== Starting Pipeline ===");
            logger.info("Kafka Broker Source: {}", kafkaBrokerSource);
            logger.info("Kafka Broker Target: {}", kafkaBrokerTarget);
            logger.info("Producer Thread Pool Size: {}", producerThreadPoolSize);
            logger.info("Consumer Thread Pool Size: {}", consumerThreadPoolSize);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Initialize producer
            Config.Producer producer = config.getProducers().get(0);
            CountDownLatch producerDone = new CountDownLatch(1);
            producerLatches.add(producerDone);

            // Create topics for each region
            Map<String, String> regionTopics = new HashMap<>();
            for (String region : config.getRegion_mapping().keySet()) {
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), region);
                String producerTopic = TopicGenerator.generateATopicName(baseTopic);
                regionTopics.put(region, producerTopic);
            }

            logger.info("[PRODUCER] Starting with configuration:");
            logger.info("[PRODUCER] - Host: {}", producer.getHost());
            logger.info("[PRODUCER] - Port: {}", producer.getPort());
            logger.info("[PRODUCER] - Namespace: {}", producer.getNamespace());
            logger.info("[PRODUCER] - Set: {}", producer.getSet());
            logger.info("[PRODUCER] - Region Topics: {}", regionTopics);

            // Start Producer
            executor.submit(() -> {
                try {
                    String[] producerArgs = new String[] {
                        kafkaBrokerSource,           // kafkaBroker
                        producer.getHost(),          // aerospikeHost
                        String.valueOf(producer.getPort()), // aerospikePort
                        producer.getNamespace(),     // namespace
                        producer.getSet(),           // setName
                        String.valueOf(maxRetries),  // maxRetries
                        String.join(",", regionTopics.values()), // topics (comma-separated list)
                        String.valueOf(producerThreadPoolSize) // workerPoolSize
                    };
                    
                    AProducer.main(producerArgs);
                } catch (Exception e) {
                    logger.error("[PRODUCER] Failed: {}", e.getMessage(), e);
                } finally {
                    producerDone.countDown();
                }
            });

            // Start Consumers for each region
            for (Map.Entry<String, List<String>> entry : config.getRegion_mapping().entrySet()) {
                String region = entry.getKey();
                List<String> consumerNames = entry.getValue();

                // Create topic and consumer group for this region
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), region);
                String consumerTopic = TopicGenerator.generateATopicName(baseTopic);
                final String consumerGroup = generateConsumerGroup(producer.getName(), region);

                logger.info("[CONSUMER] Starting consumers for region {}: {}", region, consumerNames);

                // Start each consumer for this region
                for (String consumerName : consumerNames) {
                    Config.Consumer consumer = config.getConsumers().stream()
                        .filter(c -> c.getName().equals(consumerName))
                        .findFirst()
                        .orElse(null);

                    if (consumer == null) {
                        logger.warn("[CONSUMER] Config not found for: {}", consumerName);
                        continue;
                    }

                    CountDownLatch consumerDone = new CountDownLatch(1);
                    consumerLatches.add(consumerDone);

                    // Start Consumer with corresponding consumer group
                    final String finalConsumerTopic = consumerTopic;
                    final String finalConsumerGroup = consumerGroup;
                    executor.submit(() -> {
                        try {
                            String[] consumerArgs = new String[] {
                                kafkaBrokerTarget,           // kafkaBroker
                                finalConsumerTopic,         // consumerTopic
                                finalConsumerGroup,         // consumerGroup
                                consumer.getHost(),         // aerospikeHost
                                String.valueOf(consumer.getPort()), // aerospikePort
                                consumer.getNamespace(),    // aerospikeNamespace
                                consumer.getSet(),          // aerospikeSetName
                                String.valueOf(consumerThreadPoolSize) // workerPoolSize
                            };
                            
                            logger.info("[CONSUMER] Starting {}:", consumerName);
                            logger.info("[CONSUMER] - Topic: {}", finalConsumerTopic);
                            logger.info("[CONSUMER] - Group: {}", finalConsumerGroup);
                            logger.info("[CONSUMER] - Namespace: {}", consumer.getNamespace());
                            logger.info("[CONSUMER] - Set: {}", consumer.getSet());
                            
                            AConsumer.main(consumerArgs);
                        } catch (Exception e) {
                            logger.error("[CONSUMER] {} failed: {}", consumerName, e.getMessage(), e);
                        } finally {
                            consumerDone.countDown();
                        }
                    });
                }
            }

            // Add shutdown hook to handle program termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("[MAIN] Shutting down pipeline...");
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

            // Wait for all producers and consumers to finish
            try {
                for (CountDownLatch latch : producerLatches) {
                    latch.await();
                }
                for (CountDownLatch latch : consumerLatches) {
                    latch.await();
                }
                executor.shutdown();
                logger.info("[MAIN] Pipeline completed successfully.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("[MAIN] Pipeline interrupted.");
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
