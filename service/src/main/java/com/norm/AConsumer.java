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
import java.util.ArrayList;
import java.util.List;

public class AConsumer {

    private static final AtomicInteger messagesProcessedThisSecond = new AtomicInteger(0);
    private static ExecutorService executor;
    private static volatile boolean isProcessing = true;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                            String sourceHost, int sourcePort, String sourceNamespace,
                            String destinationHost, int destinationPort, String destinationNamespace,
                            String setName, String kafkaBroker, String kafkaTopic, String consumerGroup) {
        executor = Executors.newFixedThreadPool(workerPoolSize);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AerospikeClient sourceClient = null;
        AerospikeClient destinationClient = null;

        // Tạo RateLimiter với maxMessagesPerSecond (không thêm dung sai)
        RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond);

        try {
            // Initialize Aerospike clients
            sourceClient = new AerospikeClient(sourceHost, sourcePort);
            destinationClient = new AerospikeClient(destinationHost, destinationPort);
            WritePolicy writePolicy = new WritePolicy();

            // Initialize Kafka consumer với cấu hình batch
            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // Giới hạn số lượng records trong mỗi poll
            kafkaProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800"); // 50MB
            kafkaProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1"); // Không đợi batch đầy
            kafkaProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500"); // Thời gian đợi tối đa cho mỗi fetch

            kafkaConsumer = new KafkaConsumer<>(kafkaProps);
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            final AerospikeClient finalDestinationClient = destinationClient;
            final WritePolicy finalWritePolicy = writePolicy;

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages processed in the last second: " + messagesProcessedThisSecond.get());
                messagesProcessedThisSecond.set(0);
            }, 0, 1, TimeUnit.SECONDS);

            while (isProcessing) {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) continue;

                // Xử lý batch messages
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    // Rate limit trước khi submit task
                    rateLimiter.acquire();
                    
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        try {
                            processRecord(finalDestinationClient, finalWritePolicy, record,
                                        destinationNamespace, setName);
                            messagesProcessedThisSecond.incrementAndGet();
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }, executor);
                    
                    futures.add(future);
                }

                // Đợi tất cả các futures hoàn thành
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            isProcessing = false;
            shutdownExecutor();
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
                System.out.println("Kafka Consumer closed.");
            }
            if (sourceClient != null) {
                sourceClient.close();
                System.out.println("Source Aerospike Client closed.");
            }
            if (destinationClient != null) {
                destinationClient.close();
                System.out.println("Destination Aerospike Client closed.");
            }
        }
    }

    private static void shutdownExecutor() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("Executor did not terminate in the specified time. Forcing shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Executor termination interrupted: " + e.getMessage());
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static void processRecord(AerospikeClient destinationClient, WritePolicy writePolicy,
                                    ConsumerRecord<byte[], byte[]> record, String destinationNamespace, String setName) {
        final int MAX_RETRIES = 3;
        int retryCount = 0;

        while (retryCount <= MAX_RETRIES) {
            try {
                byte[] keyBytes = record.key();
                byte[] value = record.value();

                if (keyBytes == null || value == null) {
                    System.err.println("Received null key or value, skipping record.");
                    return;
                }

                // Tạo key từ Kafka key
                String userId = new String(keyBytes);
                Key destinationKey = new Key(destinationNamespace, setName, userId);

                // Giải mã JSON từ Kafka value
                String jsonString = new String(value, StandardCharsets.UTF_8);
                Map<String, Object> data = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

                // Lấy personData và last_update từ JSON
                String personDataBase64 = (String) data.get("personData");
                byte[] personData = Base64.getDecoder().decode(personDataBase64);
                long last_update = ((Number) data.get("last_update")).longValue();

                // Tạo các bin
                Bin personBin = new Bin("personData", personData);
                Bin last_updateBin = new Bin("last_update", last_update);
                Bin keyBin = new Bin("PK", userId);

                // Ghi dữ liệu vào cơ sở dữ liệu đích
                destinationClient.put(writePolicy, destinationKey, keyBin, personBin, last_updateBin);
                return;

            } catch (Exception e) {
                retryCount++;
                System.err.println("Failed to process record (attempt " + retryCount + "): " + e.getMessage());

                if (retryCount > MAX_RETRIES) {
                    System.err.println("Max retries reached. Skipping record.");
                    e.printStackTrace();
                    return;
                }

                try {
                    // Giảm thời gian sleep giữa các lần retry
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    System.err.println("Retry sleep interrupted: " + ie.getMessage());
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
