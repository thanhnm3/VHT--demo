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

public class AConsumer {
    private static ExecutorService executor;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Semaphore processingSemaphore = new Semaphore(300); // Tăng số lượng xử lý đồng thời
    private static final AtomicLong totalProcessingTime = new AtomicLong(0);
    private static final AtomicInteger processedCount = new AtomicInteger(0);
    private static final int MONITORING_WINDOW = 1000;
    private static volatile double currentRate = 8000.0; // Tăng tốc độ ban đầu lên 8000
    private static final double MAX_RATE = 15000.0; // Tăng tốc độ tối đa
    private static final double MIN_RATE = 2000.0; // Tăng tốc độ tối thiểu
    private static final AtomicLong lastProcessedOffset = new AtomicLong(-1);
    private static final AtomicLong currentOffset = new AtomicLong(-1);
    private static final int LAG_THRESHOLD = 1000; // Ngưỡng lag để điều chỉnh tốc độ

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
                        monitorAndAdjustLag();
                        Thread.sleep(1000); // Kiểm tra mỗi giây
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
        long lag = currentOffset.get() - lastProcessedOffset.get();
        if (lag > LAG_THRESHOLD) {
            // Nếu lag vượt ngưỡng, tăng tốc độ xử lý
            currentRate = Math.min(MAX_RATE, currentRate * 1.2);
        } else if (lag < LAG_THRESHOLD / 2) {
            // Nếu lag thấp, giảm tốc độ về mức tối ưu
            currentRate = Math.max(MIN_RATE, currentRate * 0.9);
        }
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
                        Thread.sleep(10); // Giảm thời gian chờ
                        continue;
                    }

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
                        } finally {
                            processingSemaphore.release();
                        }
                    });
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
        try {
            byte[] userId = record.key();
            Key destinationKey = new Key(destinationNamespace, setName, userId);

            String jsonString = new String(record.value(), StandardCharsets.UTF_8);
            Map<String, Object> data = objectMapper.readValue(jsonString, 
                new TypeReference<Map<String, Object>>() {});

            String personDataBase64 = (String) data.get("personData");
            byte[] personData = Base64.getDecoder().decode(personDataBase64);
            long lastUpdate = ((Number) data.get("lastUpdate")).longValue();

            Bin personBin = new Bin("personData", personData);
            Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
            Bin keyBin = new Bin("PK", userId);

            destinationClient.put(writePolicy, destinationKey, keyBin, personBin, lastUpdateBin);
        } catch (Exception e) {
            throw new RuntimeException("Failed to process record: " + e.getMessage(), e);
        }
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
    }
}
