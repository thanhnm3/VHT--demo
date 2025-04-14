package com.norm;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;




public class AProducer {

    // Load configuration from .env
    private static final Dotenv dotenv = Dotenv.configure().directory("service//.env").load();

    // Aerospike configuration
    private static final String AEROSPIKE_HOST = dotenv.get("AEROSPIKE_PRODUCER_HOST");
    private static final int AEROSPIKE_PORT = Integer.parseInt(dotenv.get("AEROSPIKE_PRODUCER_PORT"));
    private static final String NAMESPACE = dotenv.get("PRODUCER_NAMESPACE");
    private static final String SET_NAME = dotenv.get("PRODUCER_SET_NAME");

    // Kafka configuration
    private static final String KAFKA_BROKER = dotenv.get("KAFKA_BROKER");
    private static final String KAFKA_TOPIC = dotenv.get("KAFKA_TOPIC");

    // Rate limiting and retry configuration
    private static final int MAX_RETRIES = Integer.parseInt(dotenv.get("MAX_RETRIES"));

    private static final AtomicInteger messagesSentThisSecond = new AtomicInteger(0);
    private static ExecutorService executor;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond) {
        executor = Executors.newFixedThreadPool(workerPoolSize); // Sử dụng workerPoolSize từ Main
        AerospikeClient aerospikeClient = null;
        KafkaProducer<String, byte[]> kafkaProducer = null;

        try {
            // Initialize Aerospike client
            aerospikeClient = new AerospikeClient(new ClientPolicy(), AEROSPIKE_HOST, AEROSPIKE_PORT);

            // Initialize Kafka producer
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", KAFKA_BROKER);
            kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            kafkaProducer = new KafkaProducer<>(kafkaProps);

            // Schedule a task to reset the message counter every second
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages sent in the last second: " + messagesSentThisSecond.get());
                messagesSentThisSecond.set(0);
            }, 0, 1, TimeUnit.SECONDS);

            // Start reading data from Aerospike and send directly to Kafka using Thread Pool
            readDataFromAerospike(aerospikeClient, kafkaProducer, maxMessagesPerSecond);

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
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate in the specified time.");
                    executor.shutdownNow(); // Buộc dừng nếu cần
                }
            } catch (InterruptedException e) {
                System.err.println("Executor termination interrupted: " + e.getMessage());
                executor.shutdownNow();
            }

            if (kafkaProducer != null) {
                kafkaProducer.close();
                System.out.println("Kafka Producer closed.");
            }
        }
    }

    private static void readDataFromAerospike(AerospikeClient client, KafkaProducer<String, byte[]> producer, int maxMessagesPerSecond) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;

        try {
            System.out.println("Starting to read data from Aerospike...");
            client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
                executor.submit(() -> {
                    try {
                        if (!record.bins.containsKey("personData")) {
                            System.err.println("Warning: Missing 'personData' bin in record: " + key.userKey);
                            return;
                        }

                        // Kiểm tra giới hạn tốc độ
                        while (messagesSentThisSecond.get() >= maxMessagesPerSecond) {
                            Thread.sleep(10); // Chờ 10ms nếu đã đạt giới hạn
                        }

                        byte[] personData = (byte[]) record.getValue("personData");
                        ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>(KAFKA_TOPIC, key.userKey.toString(), personData);

                        // Send directly to Kafka
                        sendWithRetry(producer, kafkaRecord, 0);

                        // Increment the counter for messages sent
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

    private static void sendWithRetry(KafkaProducer<String, byte[]> producer, ProducerRecord<String, byte[]> record, int retryCount) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                if (retryCount < MAX_RETRIES) {
                    System.err.println("Retrying message. Attempt: " + (retryCount + 1));
                    sendWithRetry(producer, record, retryCount + 1);
                } else {
                    System.err.println("Failed to send message after " + MAX_RETRIES + " retries: " + exception.getMessage());
                }
            }
        });
    }
}