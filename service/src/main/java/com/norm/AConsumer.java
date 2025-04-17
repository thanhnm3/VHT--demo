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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AConsumer {

    private static final AtomicInteger messagesProcessedThisSecond = new AtomicInteger(0);
    private static ExecutorService executor;
    private static volatile boolean isProcessing = true;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                            String sourceHost, int sourcePort, String sourceNamespace,
                            String destinationHost, int destinationPort, String destinationNamespace,
                            String setName, String kafkaBroker, String kafkaTopic, String consumerGroup) {
        executor = Executors.newFixedThreadPool(workerPoolSize);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AerospikeClient sourceClient = null;
        AerospikeClient destinationClient = null;

        // Tạo RateLimiter với maxMessagesPerSecond
        RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond + 300); // Thêm một chút dung sai

        try {
            // Initialize Aerospike clients
            sourceClient = new AerospikeClient(sourceHost, sourcePort);
            destinationClient = new AerospikeClient(destinationHost, destinationPort);
            WritePolicy writePolicy = new WritePolicy();

            // Initialize Kafka consumer
            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

            kafkaConsumer = new KafkaConsumer<>(kafkaProps);
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            final AerospikeClient finalSourceClient = sourceClient;
            final AerospikeClient finalDestinationClient = destinationClient;
            final WritePolicy finalWritePolicy = writePolicy;

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages processed in the last second: " + messagesProcessedThisSecond.get());
                messagesProcessedThisSecond.set(0);
            }, 0, 1, TimeUnit.SECONDS);

            while (isProcessing) {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    executor.submit(() -> {
                        try {
                            // Sử dụng RateLimiter để kiểm soát tốc độ
                            rateLimiter.acquire(); // Chờ cho đến khi có "phép" xử lý tiếp theo

                            processRecord(finalSourceClient, finalDestinationClient, finalWritePolicy, record,
                                          sourceNamespace, destinationNamespace, setName);
                            messagesProcessedThisSecond.incrementAndGet();
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + e.getMessage());
                            e.printStackTrace();
                        }
                    });
                }
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            isProcessing = false; // Đặt cờ khi không còn dữ liệu cần xử lý
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

    private static void processRecord(AerospikeClient sourceClient, AerospikeClient destinationClient, WritePolicy writePolicy,
                                      ConsumerRecord<byte[], byte[]> record, String sourceNamespace, String destinationNamespace, String setName) {
        final int MAX_RETRIES = 3; // Số lần retry tối đa
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
                Key sourceKey = new Key(sourceNamespace, setName, userId);
                Key destinationKey = new Key(destinationNamespace, setName, userId);

                // Giải mã JSON từ Kafka value
                String jsonString = new String(value, StandardCharsets.UTF_8);
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> data = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

                // Tách các trường từ JSON
                String personDataBase64 = (String) data.get("personData");
                byte[] personData = Base64.getDecoder().decode(personDataBase64);
                long migratedGen = ((Number) data.get("migratedGen")).longValue();
                int gen = ((Number) data.get("gen")).intValue(); // Lấy gen từ message

                // Tạo các bin
                Bin personBin = new Bin("personData", personData);
                Bin migratedGenBin = new Bin("migrated_gen", migratedGen);

                // Thực hiện đồng thời hai thao tác
                executor.submit(() -> {
                    try {
                        // Cập nhật migrated_gen trong cơ sở dữ liệu nguồn
                        Bin updatedMigratedGenBin = new Bin("migrated_gen", gen);
                        sourceClient.put(writePolicy, sourceKey, updatedMigratedGenBin);
                    } catch (Exception e) {
                        System.err.println("Error updating migrated_gen in source database: " + e.getMessage());
                        e.printStackTrace();
                    }
                });

                executor.submit(() -> {
                    try {
                        // Ghi dữ liệu vào cơ sở dữ liệu đích
                        destinationClient.put(writePolicy, destinationKey, personBin, migratedGenBin);
                    } catch (Exception e) {
                        System.err.println("Error writing to destination database: " + e.getMessage());
                        e.printStackTrace();
                    }
                });

                return; // Thoát nếu thành công

            } catch (Exception e) {
                retryCount++;
                System.err.println("Failed to process record (attempt " + retryCount + "): " + e.getMessage());

                if (retryCount > MAX_RETRIES) {
                    System.err.println("Max retries reached. Skipping record.");
                    e.printStackTrace();
                    return; // Thoát nếu đã retry đủ số lần
                }

                try {
                    // Chờ một khoảng thời gian trước khi retry
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    System.err.println("Retry sleep interrupted: " + ie.getMessage());
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
