package com.example.pipeline;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.AerospikeService;
import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.CdcService;
import com.example.pipeline.service.TopicGenerator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CdcConsumer.class);
    private static volatile boolean isShuttingDown = false;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
    
    // Static configuration parameters
    private static String kafkaBroker;
    private static String consumerTopic;
    private static String consumerGroup;
    private static String sourceHost;
    private static int sourcePort;
    private static String sourceNamespace;
    private static String destinationHost;
    private static int destinationPort;
    private static int workerPoolSize;
    private static int maxMessagesPerSecond;
    
    private static ConfigurationService configService;
    private static AerospikeService aerospikeService;
    private static KafkaConsumerService kafkaService;
    private static Map<String, CdcService> cdcServices;
    private static Map<String, ExecutorService> workerPools;
    private static Map<String, AtomicInteger> messagesProcessed;
    private static ScheduledExecutorService scheduler;

    public static void main(String[] args) {
        try {
            if (args.length < 10) {
                System.err.println("Usage: java CdcConsumer <kafkaBroker> <consumerTopic> <consumerGroup> " +
                                 "<sourceHost> <sourcePort> <sourceNamespace> " +
                                 "<destinationHost> <destinationPort> <workerPoolSize> <maxMessagesPerSecond>");
                System.exit(1);
            }

            // Initialize static configuration
            kafkaBroker = args[0];
            consumerTopic = args[1];
            consumerGroup = args[2];
            sourceHost = args[3];
            sourcePort = Integer.parseInt(args[4]);
            sourceNamespace = args[5];
            destinationHost = args[6];
            destinationPort = Integer.parseInt(args[7]);
            workerPoolSize = Integer.parseInt(args[8]);
            maxMessagesPerSecond = Integer.parseInt(args[9]);

            // Initialize services
            configService = ConfigurationService.getInstance();
            if (configService == null) {
                throw new IllegalStateException("Cannot initialize configuration service");
            }

            aerospikeService = new AerospikeService(destinationHost, destinationPort);
            kafkaService = new KafkaConsumerService(kafkaBroker, configService);
            kafkaService.initializeConsumers(sourceNamespace, workerPoolSize);

            cdcServices = new HashMap<>();
            workerPools = new HashMap<>();
            messagesProcessed = new HashMap<>();
            scheduler = Executors.newScheduledThreadPool(1);

            // Initialize stats monitoring
            initializeStatsMonitoring();

            // Start the consumer
            start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(CdcConsumer::shutdown));

            // Wait for shutdown signal
            shutdownLatch.await();

        } catch (Exception e) {
            logger.error("Error in main: {}", e.getMessage(), e);
        }
    }

    private static void initializeStatsMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            StringBuilder stats = new StringBuilder("\n=== Consumer Stats ===\n");
            for (Map.Entry<String, AtomicInteger> entry : messagesProcessed.entrySet()) {
                if (entry.getValue().get() > 0) {
                    stats.append(String.format("  Prefix %s: %d messages/s\n", 
                        entry.getKey(), entry.getValue().get()));
                    entry.getValue().set(0);
                }
            }
            stats.append("====================\n");
            logger.info(stats.toString());
        }, 0, 5, TimeUnit.SECONDS);
    }

    private static void start() {
        try {
            logger.info("Starting CDC consumer with:");
            logger.info("Kafka Broker: {}", kafkaBroker);
            logger.info("Source Host: {}", sourceHost);
            logger.info("Source Port: {}", sourcePort);
            logger.info("Source Namespace: {}", sourceNamespace);
            logger.info("Destination Host: {}", destinationHost);
            logger.info("Destination Port: {}", destinationPort);
            logger.info("Worker Pool Size: {}", workerPoolSize);
            logger.info("Max Messages Per Second: {}", maxMessagesPerSecond);
            logger.info("Consumer Group: {}", consumerGroup);

            Map<String, List<String>> prefixMapping = configService.getPrefixMappings();
            Map<String, String> prefixToTopicMap = kafkaService.getPrefixToTopicMap();
            
            for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();
                
                if (consumerNames.isEmpty()) {
                    logger.warn("No consumers found for prefix {}", prefix);
                    continue;
                }

                String consumerName = consumerNames.get(0);
                Config.Consumer consumer = configService.getConsumerConfig(consumerName);
                if (consumer == null) {
                    logger.warn("No consumer config found for {}", consumerName);
                    continue;
                }

                String topic = prefixToTopicMap.get(prefix);
                if (topic == null) {
                    logger.warn("No topic found for prefix {}", prefix);
                    continue;
                }
                
                String cdcTopic = TopicGenerator.generateCdcTopicName(topic);
                logger.info("[CDC Consumer] Starting consumer for prefix {}", prefix);
                logger.info("[CDC Consumer] Subscribing to topic: source-kafka.{}", cdcTopic);
                
                KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaService.createConsumer(
                    "source-kafka." + cdcTopic, consumerGroup);
                
                CdcService cdcService = new CdcService(
                    aerospikeService.getClient(),
                    aerospikeService.getWritePolicy(),
                    consumer.getNamespace(),
                    consumer.getSet()
                );
                cdcServices.put(prefix, cdcService);
                workerPools.put(prefix, Executors.newFixedThreadPool(workerPoolSize));
                messagesProcessed.put(prefix, new AtomicInteger(0));
                
                final String currentPrefix = prefix;
                new Thread(() -> {
                    try {
                        while (!isShuttingDown) {
                            var records = kafkaConsumer.poll(java.time.Duration.ofMillis(100));
                            for (var record : records) {
                                workerPools.get(currentPrefix).submit(() -> {
                                    try {
                                        cdcService.processRecord(record);
                                        messagesProcessed.get(currentPrefix).incrementAndGet();
                                    } catch (Exception e) {
                                        logger.error("[{}] Error processing record: {}", 
                                            currentPrefix, e.getMessage(), e);
                                    }
                                });
                            }
                        }
                    } catch (Exception e) {
                        logger.error("[{}] Error in CDC service: {}", 
                            currentPrefix, e.getMessage(), e);
                    }
                }, currentPrefix + "-cdc-service").start();
            }
            
        } catch (Exception e) {
            logger.error("Error starting CDC consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start CDC consumer", e);
        }
    }

    public static void shutdown() {
        logger.info("Starting graceful shutdown...");
        isShuttingDown = true;
        shutdownLatch.countDown();

        // Shutdown worker pools
        for (ExecutorService pool : workerPools.values()) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        scheduler.shutdown();
        kafkaService.shutdown();
        aerospikeService.shutdown();
        
        logger.info("Shutdown completed successfully");
    }
}
