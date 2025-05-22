package com.example.pipeline;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.util.concurrent.RateLimiter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.example.pipeline.service.RateControlService;
import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.TopicGenerator;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.ConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Base64;
import java.util.Collections;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;

public class AConsumer {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double MAX_RATE = 10000.0;
    private static final double MIN_RATE = 2000.0;
    private static final int LAG_THRESHOLD = 1000;
    private static final int MONITORING_INTERVAL_SECONDS = 10;
    private static volatile boolean isShuttingDown = false;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private static Map<String, String> prefixToTopicMap;
    private static Config config;

    // Consumer metrics for each prefix
    private static class ConsumerMetrics {
        volatile double currentRate;
        final RateLimiter rateLimiter;
        final RateControlService rateControlService;
        final KafkaConsumerService kafkaService;
        final KafkaConsumer<byte[], byte[]> consumer;
        final ThreadPoolExecutor workers;
        final String targetNamespace;
        final String prefix;
        final String setName;
        volatile boolean isRunning = true;

        ConsumerMetrics(String sourceNamespace, String kafkaBroker, 
                       String consumerGroup, String targetNamespace,
                       String prefix, String setName, int workerPoolSize) {
            this.currentRate = 8000.0;
            this.rateLimiter = RateLimiter.create(currentRate);
            this.rateControlService = new RateControlService(currentRate, MAX_RATE, MIN_RATE, 
                                                           LAG_THRESHOLD, MONITORING_INTERVAL_SECONDS);
            
            // Get topic from prefix mapping
            String topic = prefixToTopicMap.get(prefix);
            if (topic == null) {
                throw new IllegalArgumentException("No topic mapping found for prefix: " + prefix);
            }
            
            // Khi tạo consumer cho target Kafka (kafka thứ 2)
            String mirroredTopic = "source-kafka." + topic; // topic replicate từ source sang target
            this.kafkaService = new KafkaConsumerService(kafkaBroker, mirroredTopic, consumerGroup);
            this.consumer = this.kafkaService.createConsumer();
            this.consumer.subscribe(Collections.singletonList(mirroredTopic));
            this.targetNamespace = targetNamespace;
            this.prefix = prefix;
            this.setName = setName;
            
            // Create worker pool with CallerRunsPolicy
            this.workers = new ThreadPoolExecutor(
                workerPoolSize,
                workerPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadFactory() {
                    private final AtomicInteger threadCount = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName(prefix + "-worker-" + threadCount.getAndIncrement());
                        return thread;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
        }

        void monitorAndAdjustLag() {
            double newRate = rateControlService.calculateNewRateForConsumer(
                kafkaService.getCurrentOffset(), 
                kafkaService.getLastProcessedOffset()
            );
            rateControlService.updateRate(newRate);
            currentRate = rateControlService.getCurrentRate();
            rateLimiter.setRate(currentRate);
        }

        void shutdown() {
            isRunning = false;
            if (rateControlService != null) {
                rateControlService.shutdown();
            }
            if (kafkaService != null) {
                kafkaService.shutdown();
            }
            if (consumer != null) {
                consumer.wakeup();
                consumer.close();
            }
            if (workers != null) {
                workers.shutdown();
                try {
                    if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                        workers.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    workers.shutdownNow();
                }
            }
        }
    }

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String sourceHost, int sourcePort, String sourceNamespace,
                          String destinationHost, int destinationPort, 
                          String consumerNamespace096, String consumerNamespace033,
                          String consumerSetName096, String consumerSetName033,
                          String kafkaBroker, String consumerGroup096, String consumerGroup033) {
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

                // Determine consumer group based on prefix
                String consumerGroup = prefix.equals("096") ? consumerGroup096 : consumerGroup033;
                String consumerNamespace = prefix.equals("096") ? consumerNamespace096 : consumerNamespace033;
                String consumerSetName = prefix.equals("096") ? consumerSetName096 : consumerSetName033;

                metricsMap.put(prefix, new ConsumerMetrics(
                    sourceNamespace, kafkaBroker,
                    consumerGroup, consumerNamespace,
                    prefix, consumerSetName, workerPoolSize
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
            while (!Thread.currentThread().isInterrupted() && metrics.isRunning) {
                try {
                    if (metrics.rateControlService.shouldCheckRateAdjustment()) {
                        double oldRate = metrics.currentRate;
                        metrics.monitorAndAdjustLag();
                        if (oldRate != metrics.currentRate) {
                            System.out.printf("[%s] Rate adjusted from %.2f to %.2f messages/second%n", 
                                            metrics.prefix, oldRate, metrics.currentRate);
                        }
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, metrics.prefix + "-monitor").start();
    }

    private static void startProcessingThread(ConsumerMetrics metrics,
                                           AerospikeClient destinationClient,
                                           WritePolicy writePolicy) {
        Thread processorThread = new Thread(() -> {
            processMessages(metrics.consumer, destinationClient, writePolicy,
                          metrics.targetNamespace, metrics.setName, metrics.rateLimiter,
                          metrics.workers, metrics.currentRate,
                          metrics.rateControlService, metrics.prefix);
        });
        processorThread.setName(metrics.prefix + "-processor");
        processorThread.start();
    }

    private static void processMessages(KafkaConsumer<byte[], byte[]> consumer,
                                     AerospikeClient destinationClient,
                                     WritePolicy writePolicy,
                                     String destinationNamespace,
                                     String setName,
                                     RateLimiter rateLimiter,
                                     ThreadPoolExecutor workers,
                                     double currentRate,
                                     RateControlService rateControlService,
                                     String prefix) {
        int emptyPollCount = 0;
        final int maxEmptyPolls = 100; // 100 * 100ms = 10s

        try {
            while (!Thread.currentThread().isInterrupted() && !isShuttingDown) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    emptyPollCount = 0; // reset nếu có message
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        try {
                            if (currentRate < rateControlService.getTargetRate() * 0.8) {
                                processRecord(record, destinationClient, writePolicy, 
                                            destinationNamespace, setName);
                            } else {
                                workers.submit(() -> {
                                    try {
                                        processRecord(record, destinationClient, writePolicy, 
                                                    destinationNamespace, setName);
                                    } catch (Exception e) {
                                        System.err.printf("[%s] Error processing record: %s%n", 
                                                        prefix, e.getMessage());
                                    }
                                });
                            }
                        } catch (Exception e) {
                            System.err.printf("[%s] Error in message processing: %s%n", 
                                            prefix, e.getMessage());
                        }
                    }
                    
                    // Commit offsets after successful batch processing
                    try {
                        consumer.commitSync();
                    } catch (Exception e) {
                        System.err.printf("[%s] Error committing offsets: %s%n", 
                                        prefix, e.getMessage());
                    }
                } else {
                    emptyPollCount++;
                    if (emptyPollCount >= maxEmptyPolls) {
                        System.out.printf("[%s] No messages received for 10 seconds. Initiating shutdown.%n", prefix);
                        isShuttingDown = true;
                        shutdownLatch.countDown();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            if (!isShuttingDown) {
                System.err.printf("[%s] Error in message processing: %s%n", prefix, e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static void processRecord(ConsumerRecord<byte[], byte[]> record,
                                    AerospikeClient destinationClient,
                                    WritePolicy writePolicy,
                                    String destinationNamespace,
                                    String setName) {
        int maxRetries = 3;
        int currentRetry = 0;
        boolean success = false;
        String recordKey = new String(record.key(), StandardCharsets.UTF_8);

        while (!success && currentRetry < maxRetries) {
            try {
                if (!validateConnection(destinationClient)) {
                    System.err.printf("Aerospike connection error for record %s: Connection is not valid%n", recordKey);
                    throw new RuntimeException("Aerospike connection is not valid");
                }

                if (record.key() == null || record.value() == null) {
                    System.err.printf("Invalid record %s: key or value is null%n", recordKey);
                    throw new IllegalArgumentException("Invalid record: key or value is null");
                }

                byte[] userId = record.key();
                Key destinationKey = new Key(destinationNamespace, setName, userId);

                String jsonString = new String(record.value(), StandardCharsets.UTF_8);
                Map<String, Object> data = objectMapper.readValue(jsonString, 
                    new TypeReference<Map<String, Object>>() {});

                if (!data.containsKey("personData") || !data.containsKey("lastUpdate")) {
                    System.err.printf("Invalid data format for record %s: missing required fields%n", recordKey);
                    throw new IllegalArgumentException("Invalid data format: missing required fields");
                }

                String personDataBase64 = (String) data.get("personData");
                byte[] personData = Base64.getDecoder().decode(personDataBase64);
                long lastUpdate = ((Number) data.get("lastUpdate")).longValue();

                Bin personBin = new Bin("personData", personData);
                Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
                Bin keyBin = new Bin("PK", userId);

                try {
                    destinationClient.put(writePolicy, destinationKey, keyBin, personBin, lastUpdateBin);
                    success = true;
                } catch (Exception e) {
                    if (isRetryableError(e)) {
                        System.err.printf("Retryable error for record %s: %s%n", recordKey, e.getMessage());
                        throw e;
                    } else {
                        System.err.printf("Non-retryable error for record %s: %s%n", recordKey, e.getMessage());
                        throw new RuntimeException("Non-retryable error during write: " + e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                currentRetry++;
                if (currentRetry < maxRetries) {
                    System.err.printf("Retry attempt %d for record %s: %s%n", 
                        currentRetry, recordKey, e.getMessage());
                    handleRetry(currentRetry, e);
                } else {
                    System.err.printf("Failed to process record %s after %d attempts: %s%n", 
                        recordKey, maxRetries, e.getMessage());
                    throw new RuntimeException("Failed to process record after " + maxRetries + " attempts: " + e.getMessage(), e);
                }
            }
        }
    }

    private static boolean validateConnection(AerospikeClient client) {
        try {
            return client != null && client.isConnected();
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isRetryableError(Exception e) {
        return e instanceof com.aerospike.client.AerospikeException.Timeout ||
               e instanceof com.aerospike.client.AerospikeException.Connection;
    }

    private static void handleRetry(int currentRetry, Exception e) {
        long backoffTime = calculateBackoffTime(currentRetry);
        System.err.printf("Waiting %d ms before retry attempt %d%n", backoffTime, currentRetry);
        try {
            Thread.sleep(backoffTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry", ie);
        }
    }

    private static long calculateBackoffTime(int retryCount) {
        long baseDelay = 1000;
        long maxDelay = 10000;
        long delay = Math.min(baseDelay * (1L << retryCount), maxDelay);
        return delay + (long)(Math.random() * 1000);
    }
}
