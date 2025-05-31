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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;

public class CdcConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CdcConsumer.class);
    
    // Instance configuration parameters
    private final String kafkaBroker;
    private final String consumerTopic;
    private final String consumerGroup;
    private final String destinationHost;
    private final int destinationPort;
    private final String destinationNamespace;
    private final int workerPoolSize;
    
    // Instance services
    private final ConfigurationService configService;
    private final AerospikeService aerospikeService;
    private final KafkaConsumerService kafkaService;
    private final Map<String, CdcService> cdcServices;
    private final Map<String, ExecutorService> workerPools;
    private final ExecutorService scheduler;

    public CdcConsumer(String[] args) {
        if (args.length < 7) {
            throw new IllegalArgumentException("Usage: java CdcConsumer <kafkaBroker> <consumerTopic> <consumerGroup> " +
                             "<destinationHost> <destinationPort> <destinationNamespace> <workerPoolSize>");
        }

        // Initialize configuration
        this.kafkaBroker = args[0];
        this.consumerTopic = args[1];
        this.consumerGroup = args[2];
        this.destinationHost = args[3];
        this.destinationPort = Integer.parseInt(args[4]);
        this.destinationNamespace = args[5];
        this.workerPoolSize = Integer.parseInt(args[6]);
        
        // Initialize services
        this.configService = ConfigurationService.getInstance();
        if (this.configService == null) {
            throw new IllegalStateException("Cannot initialize configuration service");
        }

        this.aerospikeService = new AerospikeService(destinationHost, destinationPort);
        this.kafkaService = new KafkaConsumerService(kafkaBroker, configService);
        this.kafkaService.initializeConsumers(destinationNamespace, workerPoolSize);
        
        this.cdcServices = new HashMap<>();
        this.workerPools = new HashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public static void main(String[] args) {
        try {
            CdcConsumer consumer = new CdcConsumer(args);
            consumer.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        } catch (Exception e) {
            logger.error("Error in main: {}", e.getMessage(), e);
        }
    }

    private void start() {
        try {
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
                
                // Sử dụng TopicGenerator để tạo consumer group cho CDC
                String topicConsumerGroup = TopicGenerator.generateCdcGroupName(topic);
                
                KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaService.createConsumer(
                    consumerTopic, topicConsumerGroup);
                
                CdcService cdcService = new CdcService(
                    aerospikeService.getClient(),
                    aerospikeService.getWritePolicy(),
                    consumer.getNamespace(),
                    consumer.getSet()
                );
                cdcServices.put(prefix, cdcService);
                workerPools.put(prefix, Executors.newFixedThreadPool(workerPoolSize));
                
                final String currentPrefix = prefix;
                new Thread(() -> {
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            var records = kafkaConsumer.poll(java.time.Duration.ofMillis(100));
                            if (!records.isEmpty()) {
                                // Tạo một CountDownLatch để đợi tất cả các record được xử lý
                                CountDownLatch processingLatch = new CountDownLatch(records.count());
                                
                                for (var record : records) {
                                    workerPools.get(currentPrefix).submit(() -> {
                                        try {
                                            cdcService.processRecord(record);
                                        } catch (Exception e) {
                                            logger.error("[{}] Error processing record: {}", 
                                                currentPrefix, e.getMessage(), e);
                                        } finally {
                                            processingLatch.countDown();
                                        }
                                    });
                                }
                                
                                // Đợi tất cả các record được xử lý
                                try {
                                    processingLatch.await(30, TimeUnit.SECONDS);
                                    // Commit offset sau khi xử lý thành công
                                    kafkaConsumer.commitSync();
                                    logger.debug("[{}] Committed offset for {} records", 
                                        currentPrefix, records.count());
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    logger.error("[{}] Interrupted while waiting for records to process", 
                                        currentPrefix);
                                }
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

    public void shutdown() {
        logger.info("Starting graceful shutdown...");

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
