package com.norm;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.util.concurrent.RateLimiter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Base64;
import java.util.Collections;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Queue;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;

public class AConsumer {
    private static ExecutorService executor;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Semaphore processingSemaphore = new Semaphore(300); // Tăng số lượng xử lý đồng thời
    private static final AtomicLong totalProcessingTime = new AtomicLong(0);
    private static final AtomicInteger processedCount = new AtomicInteger(0);
    private static final int MONITORING_WINDOW = 1000;
    private static volatile double currentRate = 8000.0; // Tăng tốc độ ban đầu lên 8000
    private static final double MAX_RATE = 10000.0; // Tăng tốc độ tối đa
    private static final double MIN_RATE = 2000.0; // Tăng tốc độ tối thiểu
    private static final AtomicLong lastProcessedOffset = new AtomicLong(-1);
    private static final AtomicLong currentOffset = new AtomicLong(-1);
    private static final int LAG_THRESHOLD = 1000; // Ngưỡng lag để điều chỉnh tốc độ
    private static final int MONITORING_INTERVAL_SECONDS = 10; // Thêm hằng số cho interval
    private static final AtomicLong lastRateAdjustmentTime = new AtomicLong(System.currentTimeMillis());
    private static final Queue<ConsumerRecord<byte[], byte[]>> pendingMessages = new ConcurrentLinkedQueue<>();
    private static final int RATE_ADJUSTMENT_STEPS = 5; // Số bước điều chỉnh rate
    private static final double MAX_RATE_CHANGE_PERCENT = 0.2; // Tối đa 20% thay đổi mỗi lần
    private static volatile double targetRate = 8000.0; // Rate mục tiêu
    private static final ScheduledExecutorService rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String sourceHost, int sourcePort, String sourceNamespace,
                          String destinationHost, int destinationPort, String destinationNamespace,
                          String setName, String kafkaBroker, String kafkaTopic, String consumerGroup) {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AerospikeClient destinationClient = null;

        try {
            // Initialize Aerospike client
            destinationClient = new AerospikeClient(destinationHost, destinationPort);
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.totalTimeout = 200; // 500ms timeout

            // Initialize Kafka consumer
            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
            kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

            kafkaConsumer = new KafkaConsumer<>(kafkaProps);
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            // Initialize worker pool with more threads
            ExecutorService workers = Executors.newFixedThreadPool(workerPoolSize ,
                new ThreadFactory() {
                    private final AtomicInteger threadCount = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("consumer-worker-" + threadCount.getAndIncrement());
                        return thread;
                    }
                });

            // Create RateLimiter with higher initial rate
            RateLimiter rateLimiter = RateLimiter.create(currentRate);

            // Start lag monitoring thread
            new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastRateAdjustmentTime.get() >= MONITORING_INTERVAL_SECONDS * 1000) {
                            double oldRate = currentRate;
                            monitorAndAdjustLag();
                            if (oldRate != currentRate) {
                                System.out.printf("Consumer rate adjusted from %.2f to %.2f messages/second%n", 
                                                oldRate, currentRate);
                                lastRateAdjustmentTime.set(currentTime);
                            }
                        }
                        Thread.sleep(1000); // Vẫn check mỗi giây nhưng chỉ điều chỉnh mỗi 10 giây
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

    private static void adjustRateSmoothly(double newTargetRate) {
        if (newTargetRate == targetRate) return;

        // Tính toán số bước và lượng thay đổi cho mỗi bước
        final double rateChange = newTargetRate - currentRate;
        final double stepSize = rateChange / RATE_ADJUSTMENT_STEPS;
        
        // Giới hạn thay đổi tối đa mỗi bước
        final double maxStepChange = currentRate * MAX_RATE_CHANGE_PERCENT;
        final double finalStepSize = Math.abs(stepSize) > maxStepChange ? 
            Math.signum(stepSize) * maxStepChange : stepSize;

        // Lên lịch điều chỉnh rate từng bước
        for (int i = 0; i < RATE_ADJUSTMENT_STEPS; i++) {
            final int step = i;
            rateAdjustmentExecutor.schedule(() -> {
                double newRate = currentRate + finalStepSize;
                if (finalStepSize > 0) {
                    currentRate = Math.min(newRate, targetRate);
                } else {
                    currentRate = Math.max(newRate, targetRate);
                }
            }, step * 1000, TimeUnit.MILLISECONDS);
        }

        targetRate = newTargetRate;
    }

    private static void monitorAndAdjustLag() {
        long lag = currentOffset.get() - lastProcessedOffset.get();
        if (lag > LAG_THRESHOLD) {
            double newRate = Math.min(MAX_RATE, currentRate * 1.2);
            adjustRateSmoothly(newRate);
        } else if (lag < LAG_THRESHOLD / 2) {
            double newRate = Math.max(MIN_RATE, currentRate * 0.9);
            adjustRateSmoothly(newRate);
        }
    }

    private static void processPendingMessages(AerospikeClient destinationClient,
                                            WritePolicy writePolicy,
                                            String destinationNamespace,
                                            String setName,
                                            ExecutorService workers) {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        while (!pendingMessages.isEmpty() && batch.size() < 100) {
            ConsumerRecord<byte[], byte[]> record = pendingMessages.poll();
            if (record != null) {
                batch.add(record);
            }
        }
        if (!batch.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> record : batch) {
                if (processingSemaphore.tryAcquire()) {
                    try {
                        processRecordWithRetry(record, destinationClient, writePolicy, 
                                            destinationNamespace, setName, workers);
                    } finally {
                        processingSemaphore.release();
                    }
                } else {
                    // Nếu không lấy được semaphore, đưa lại vào queue
                    pendingMessages.offer(record);
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
                long startTime = System.nanoTime();
                processRecord(record, destinationClient, writePolicy, 
                            destinationNamespace, setName);
                long processingTime = System.nanoTime() - startTime;
                
                lastProcessedOffset.set(record.offset());
                totalProcessingTime.addAndGet(processingTime);
                processedCount.incrementAndGet();
                
                if (processedCount.get() % MONITORING_WINDOW == 0) {
                    adjustProcessingRate();
                }
            } catch (Exception e) {
                System.err.println("Error processing record: " + e.getMessage());
                // Nếu xử lý thất bại, đưa lại vào queue
                pendingMessages.offer(record);
            }
        });
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
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10));
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    currentOffset.set(record.offset());
                    
                    if (!processingSemaphore.tryAcquire()) {
                        // Nếu không lấy được semaphore, lưu vào queue
                        pendingMessages.offer(record);
                        continue;
                    }

                    try {
                        if (currentRate < targetRate * 0.8) {
                            // Nếu rate đang giảm mạnh, lưu vào queue
                            pendingMessages.offer(record);
                        } else {
                            processRecordWithRetry(record, destinationClient, writePolicy,
                                                destinationNamespace, setName, workers);
                        }
                    } finally {
                        // Luôn giải phóng semaphore sau khi xử lý xong
                        processingSemaphore.release();
                    }

                    // Xử lý message đang chờ khi có cơ hội
                    if (!pendingMessages.isEmpty() && currentRate >= targetRate * 0.9) {
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

    private static void adjustProcessingRate() {
        int count = processedCount.get();
        if (count == 0) return;

        long totalTime = totalProcessingTime.get();
        double avgProcessingTime = (double) totalTime / count / 1_000_000_000.0;
        
        // Điều chỉnh tốc độ tích cực hơn
        if (avgProcessingTime > 0.1) { // Nếu xử lý chậm
            currentRate = Math.max(MIN_RATE, currentRate * 0.95); // Giảm 5%
        } else if (avgProcessingTime < 0.05) { // Nếu xử lý nhanh
            currentRate = Math.min(MAX_RATE, currentRate * 1.1); // Tăng 10%
        } else if (avgProcessingTime < 0.02) { // Nếu xử lý rất nhanh
            currentRate = Math.min(MAX_RATE, currentRate * 1.2); // Tăng 20%
        }

        // Reset metrics
        totalProcessingTime.set(0);
        processedCount.set(0);
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
                // Validate connection
                if (!validateConnection(destinationClient)) {
                    System.err.printf("Aerospike connection error for record %s: Connection is not valid%n", recordKey);
                    throw new RuntimeException("Aerospike connection is not valid");
                }

                // Validate input data
                if (record.key() == null || record.value() == null) {
                    System.err.printf("Invalid record %s: key or value is null%n", recordKey);
                    throw new IllegalArgumentException("Invalid record: key or value is null");
                }

                byte[] userId = record.key();
                Key destinationKey = new Key(destinationNamespace, setName, userId);

                // Parse and validate JSON data
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

                // Create bins with validation
                Bin personBin = new Bin("personData", personData);
                Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
                Bin keyBin = new Bin("PK", userId);

                // Execute write with transaction
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
        // Add specific error types that should be retried
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
        // Exponential backoff with jitter
        long baseDelay = 1000; // 1 second
        long maxDelay = 10000; // 10 seconds
        long delay = Math.min(baseDelay * (1L << retryCount), maxDelay);
        return delay + (long)(Math.random() * 1000); // Add jitter
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

        if (rateAdjustmentExecutor != null) {
            rateAdjustmentExecutor.shutdown();
            try {
                if (!rateAdjustmentExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                    rateAdjustmentExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                rateAdjustmentExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
