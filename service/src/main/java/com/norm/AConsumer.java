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

public class AConsumer {
    private static ExecutorService executor;
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
            RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond*2);

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

    private static void processMessages(KafkaConsumer<byte[], byte[]> consumer,
                                     AerospikeClient destinationClient,
                                     WritePolicy writePolicy,
                                     String destinationNamespace,
                                     String setName,
                                     RateLimiter rateLimiter,
                                     ExecutorService workers) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(0));
                
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    workers.submit(() -> {
                        try {
                            processRecord(record, destinationClient, writePolicy, 
                                        destinationNamespace, setName);
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + e.getMessage());
                        }
                    });
                }
            }
        } catch (Exception e) {
            System.err.println("Error in message processing: " + e.getMessage());
            e.printStackTrace();
        }
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
