package com.norm;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.util.concurrent.RateLimiter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.norm.service.RateControlService;
import com.norm.service.KafkaService;
import com.norm.service.MessageService;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Base64;
import java.util.Collections;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;

public class AConsumer {
    private static ExecutorService executor;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Semaphore processingSemaphore = new Semaphore(300);
    private static volatile double currentRate = 8000.0;
    private static final double MAX_RATE = 10000.0;
    private static final double MIN_RATE = 2000.0;
    private static final int LAG_THRESHOLD = 1000;
    private static final int MONITORING_INTERVAL_SECONDS = 10;
    private static RateControlService rateControlService;
    private static KafkaService kafkaService;
    private static MessageService messageService;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String sourceHost, int sourcePort, String sourceNamespace,
                          String destinationHost, int destinationPort, String destinationNamespace,
                          String setName, String kafkaBroker, String kafkaTopic, String consumerGroup) {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AerospikeClient destinationClient = null;

        try {
            // Initialize services
            rateControlService = new RateControlService(8000.0, MAX_RATE, MIN_RATE, 
                                                      LAG_THRESHOLD, MONITORING_INTERVAL_SECONDS);
            kafkaService = new KafkaService(kafkaBroker, kafkaTopic, consumerGroup);
            messageService = new MessageService();

            // Initialize Aerospike client
            destinationClient = new AerospikeClient(destinationHost, destinationPort);
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.totalTimeout = 200;

            // Initialize Kafka consumer
            kafkaConsumer = kafkaService.createConsumer();

            // Initialize worker pool
            ExecutorService workers = Executors.newFixedThreadPool(workerPoolSize,
                new ThreadFactory() {
                    private final AtomicInteger threadCount = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("consumer-worker-" + threadCount.getAndIncrement());
                        return thread;
                    }
                });

            // Create RateLimiter
            RateLimiter rateLimiter = RateLimiter.create(currentRate);

            // Start lag monitoring thread
            new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (rateControlService.shouldCheckRateAdjustment()) {
                            double oldRate = currentRate;
                            monitorAndAdjustLag();
                            if (oldRate != currentRate) {
                                System.out.printf("Consumer rate adjusted from %.2f to %.2f messages/second%n", 
                                                oldRate, currentRate);
                            }
                        }
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }).start();

            // Start processing
            processMessages(kafkaConsumer, destinationClient, writePolicy, 
                          destinationNamespace, setName, rateLimiter, workers);

        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownGracefully(kafkaConsumer, destinationClient);
        }
    }

    private static void monitorAndAdjustLag() {
        double newRate = rateControlService.calculateNewRateForConsumer(
            kafkaService.getCurrentOffset(), 
            kafkaService.getLastProcessedOffset()
        );
        rateControlService.updateRate(newRate);
        currentRate = rateControlService.getCurrentRate();
    }

    private static void processMessages(KafkaConsumer<byte[], byte[]> consumer,
                                     AerospikeClient destinationClient,
                                     WritePolicy writePolicy,
                                     String destinationNamespace,
                                     String setName,
                                     RateLimiter rateLimiter,
                                     ExecutorService workers) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<byte[], byte[]> records = messageService.pollMessages(consumer);
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    kafkaService.updateOffsets(record.offset(), record.offset());
                    
                    if (!processingSemaphore.tryAcquire()) {
                        messageService.processPendingConsumerMessages(Collections.singletonList(record));
                        continue;
                    }

                    try {
                        if (currentRate < rateControlService.getTargetRate() * 0.8) {
                            messageService.processPendingConsumerMessages(Collections.singletonList(record));
                        } else {
                            processRecordWithRetry(record, destinationClient, writePolicy, 
                                                destinationNamespace, setName, workers);
                        }
                    } finally {
                        processingSemaphore.release();
                    }

                    if (messageService.hasPendingConsumerMessages() && 
                        currentRate >= rateControlService.getTargetRate() * 0.9) {
                        processPendingMessages(destinationClient, writePolicy,
                                            destinationNamespace, setName, workers);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error in message processing: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processPendingMessages(AerospikeClient destinationClient,
                                            WritePolicy writePolicy,
                                            String destinationNamespace,
                                            String setName,
                                            ExecutorService workers) {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        ConsumerRecord<byte[], byte[]> record;
        while ((record = messageService.pollConsumerMessage()) != null && batch.size() < 100) {
            batch.add(record);
        }
        if (!batch.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> r : batch) {
                if (processingSemaphore.tryAcquire()) {
                    try {
                        processRecordWithRetry(r, destinationClient, writePolicy, 
                                            destinationNamespace, setName, workers);
                    } finally {
                        processingSemaphore.release();
                    }
                } else {
                    messageService.processPendingConsumerMessages(Collections.singletonList(r));
                }
            }
        }
    }

    private static void processRecordWithRetry(ConsumerRecord<byte[], byte[]> record,
                                            AerospikeClient destinationClient,
                                            WritePolicy writePolicy,
                                            String destinationNamespace,
                                            String setName,
                                            ExecutorService workers) {
        workers.submit(() -> {
            try {
                processRecord(record, destinationClient, writePolicy, 
                            destinationNamespace, setName);
                kafkaService.updateOffsets(record.offset(), record.offset());
            } catch (Exception e) {
                System.err.println("Error processing record: " + e.getMessage());
                messageService.processPendingConsumerMessages(Collections.singletonList(record));
            }
        });
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

    private static void shutdownGracefully(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                         AerospikeClient aerospikeClient) {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (rateControlService != null) {
            rateControlService.shutdown();
        }

        if (kafkaService != null) {
            kafkaService.shutdown();
        }

        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.close();
                System.out.println("Kafka Consumer closed successfully.");
            } catch (Exception e) {
                System.err.println("Error closing Kafka consumer: " + e.getMessage());
            }
        }

        if (aerospikeClient != null) {
            try {
                aerospikeClient.close();
                System.out.println("Aerospike Client closed successfully.");
            } catch (Exception e) {
                System.err.println("Error closing Aerospike client: " + e.getMessage());
            }
        }
    }
}
