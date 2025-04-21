package com.norm;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.google.common.util.concurrent.RateLimiter;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AProducer {


    private static final AtomicInteger messagesSentThisSecond = new AtomicInteger(0);
    private static ExecutorService executor;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                            String aerospikeHost, int aerospikePort, String namespace, String setName,
                            String kafkaBroker, String kafkaTopic, int maxRetries) {
        // Sử dụng các tham số được truyền từ Main.java
        AerospikeClient aerospikeClient = null;
        KafkaProducer<String, byte[]> kafkaProducer = null;

        try {
            // Initialize Aerospike client
            aerospikeClient = new AerospikeClient(new ClientPolicy(), aerospikeHost, aerospikePort);

            // Initialize Kafka producer
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", kafkaBroker);
            kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            kafkaProps.put("linger.ms", "5"); // Thời gian chờ để gom batch
            kafkaProps.put("batch.size", "32768"); // Kích thước batch tối đa (32KB)
            kafkaProps.put("acks", "all"); // Đảm bảo tất cả các bản sao nhận được dữ liệu
            kafkaProducer = new KafkaProducer<>(kafkaProps);

            // Initialize thread pool
            executor = Executors.newFixedThreadPool(workerPoolSize);

            // Schedule a task to reset the message counter every second
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages sent in the last second: " + messagesSentThisSecond.get());
                messagesSentThisSecond.set(0);
            }, 0, 1, TimeUnit.SECONDS);

            // Start reading data from Aerospike and send directly to Kafka using Thread Pool
            readDataFromAerospike(aerospikeClient, kafkaProducer, maxMessagesPerSecond, namespace, setName, kafkaTopic, maxRetries);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (aerospikeClient != null) {
                aerospikeClient.close();
            }

            // Đợi tất cả các thread trong ThreadPool hoàn thành trước khi đóng KafkaProducer
            executor.shutdown();
            try {
                // Tăng thời gian chờ để đảm bảo tất cả các tác vụ hoàn thành
                if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                    System.err.println("Executor did not terminate in the specified time. Forcing shutdown...");
                    executor.shutdownNow(); // Buộc dừng nếu cần
                }
            } catch (InterruptedException e) {
                System.err.println("Executor termination interrupted: " + e.getMessage());
                executor.shutdownNow(); // Buộc dừng nếu bị gián đoạn
                Thread.currentThread().interrupt(); // Khôi phục trạng thái gián đoạn
            }

            if (kafkaProducer != null) {
                kafkaProducer.close();
                System.out.println("Kafka Producer closed.");
            }
        }
    }

    private static void readDataFromAerospike(AerospikeClient client, KafkaProducer<String, byte[]> producer,
                                              int maxMessagesPerSecond, String namespace, String setName,
                                              String kafkaTopic, int maxRetries) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;

        // Tạo RateLimiter với maxMessagesPerSecond
        RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond);

        try {
            System.out.println("Starting to read data from Aerospike...");
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                executor.submit(() -> {
                    try {
                        if (!record.bins.containsKey("personData") || !record.bins.containsKey("migrated_gen")) {
                            System.err.println("Warning: Missing required bins in record: " + key.userKey);
                            return;
                        }

                        rateLimiter.acquire();
                        // Không kiểm tra bins nữa, lấy giá trị luôn (có thể là null)
                        byte[] personData = (byte[]) record.getValue("personData");
                        long lastUpdate = System.currentTimeMillis(); // Lấy timestamp hiện tại

                        // Kết hợp personData và lastUpdate thành một gói dữ liệu JSON
                        String message = String.format("{\"personData\": \"%s\", \"lastUpdate\": %d}",
                                Base64.getEncoder().encodeToString(personData), lastUpdate);

                        // Tạo Kafka record và gửi dữ liệu
                        ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>(
                                kafkaTopic,
                                key.userKey.toString(), // Key từ Aerospike
                                message.getBytes(StandardCharsets.UTF_8) // Giá trị là JSON chứa personData và lastUpdate
                        );

                        // Gửi dữ liệu tới Kafka
                        sendWithRetry(producer, kafkaRecord, 0, maxRetries);

                        // Tăng bộ đếm số lượng message đã gửi
                        messagesSentThisSecond.incrementAndGet();
                    } catch (Exception e) {
                        System.err.println("Error processing record: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            });
            System.out.println("Finished scanning data from Aerospike.");
        } catch (Exception e) {
            System.err.println("Error scanning data from Aerospike: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void sendWithRetry(KafkaProducer<String, byte[]> producer, ProducerRecord<String, byte[]> record,
                                      int retryCount, int maxRetries) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                if (retryCount < maxRetries) {
                    System.err.println("Retrying message. Attempt: " + (retryCount + 1));
                    sendWithRetry(producer, record, retryCount + 1, maxRetries);
                } else {
                    System.err.println("Failed to send message after " + maxRetries + " retries: " + exception.getMessage());
                }
            }
        });
    }
}

