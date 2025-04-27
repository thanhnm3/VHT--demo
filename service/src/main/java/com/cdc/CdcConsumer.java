package com.cdc;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.google.common.util.concurrent.RateLimiter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Base64;
import java.util.Collections;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;

public class CdcConsumer {





    private static final AtomicInteger messagesProcessedThisSecond = new AtomicInteger(0);
    private static ExecutorService executor;

    public static void main(String aeroHost, int aeroPort, String namespace, String setName, String kafkaBroker,
                           String kafkaTopic, String groupId, int workerPoolSize, int maxMessagesPerSecond) {
        executor = Executors.newFixedThreadPool(workerPoolSize);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AerospikeClient aerospikeClient = null;

        // Tạo RateLimiter với maxMessagesPerSecond
        RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond);

        try {
            aerospikeClient = new AerospikeClient(aeroHost, aeroPort);
            WritePolicy writePolicy = new WritePolicy();

            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

            // Thêm các cấu hình tối ưu hóa
            kafkaProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "32768"); // Gom đủ 32KB trước khi gửi batch
            kafkaProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "50");  // Chờ tối đa 50ms để gom batch
            kafkaProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576"); // Tăng giới hạn lên 1MB cho mỗi partition

            kafkaConsumer = new KafkaConsumer<>(kafkaProps);
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            final AerospikeClient finalAerospikeClient = aerospikeClient;
            final WritePolicy finalWritePolicy = writePolicy;

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages consumed: " + messagesProcessedThisSecond.get());
                messagesProcessedThisSecond.set(0);
            }, 0, 1, TimeUnit.SECONDS);

            while (true) {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    executor.submit(() -> {
                        try {
                            // Sử dụng RateLimiter để kiểm soát tốc độ
                            rateLimiter.acquire(); // Chờ cho đến khi có "phép" xử lý tiếp theo

                            processRecord(finalAerospikeClient, finalWritePolicy, record, namespace, setName);
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
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
                System.out.println("Kafka Consumer closed.");
            }
            if (aerospikeClient != null) {
                aerospikeClient.close();
                System.out.println("Aerospike Client closed.");
            }
            executor.shutdown();
        }
    }

    private static void processRecord(AerospikeClient aerospikeClient, WritePolicy writePolicy, ConsumerRecord<byte[], byte[]> record,
                                      String namespace, String setName) {
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
                byte[] userId = record.key();
                Key aerospikeKey = new Key(namespace, setName, userId);

                // Giải mã JSON từ Kafka value
                String jsonString = new String(value, StandardCharsets.UTF_8);
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, Object> data = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});

                // Tách các trường từ JSON
                String personDataBase64 = (String) data.get("personData");
                byte[] personData = null;
                if (personDataBase64 != null) {
                    try {
                        personData = Base64.getDecoder().decode(personDataBase64);
                    } catch (IllegalArgumentException e) {
                        System.err.println("Invalid Base64 in personData, inserting as null.");
                        personData = null;
                    }
                }
                // Nếu personDataBase64 là null hoặc decode lỗi thì personData vẫn là null và vẫn insert

                Object lastUpdateObj = data.get("lastUpdate");
                if (lastUpdateObj == null) {
                    System.err.println("lastUpdate is null, skipping record.");
                    return;
                }
                long lastUpdate;
                try {
                    lastUpdate = ((Number) lastUpdateObj).longValue();
                } catch (Exception e) {
                    System.err.println("Invalid lastUpdate value, skipping record.");
                    return;
                }

                // Tạo các bin
                Bin personBin = new Bin("personData", personData);
                Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);

                aerospikeClient.put(writePolicy, aerospikeKey, personBin, lastUpdateBin);
                return; // Ghi thành công, thoát khỏi vòng lặp

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
                    Thread.sleep(100 * retryCount); // Tăng thời gian chờ theo số lần retry
                } catch (InterruptedException ie) {
                    System.err.println("Retry sleep interrupted: " + ie.getMessage());
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
