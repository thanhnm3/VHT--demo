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
import java.util.ArrayList;
import java.util.List;

public class AConsumer {
    private static final AtomicInteger messagesProcessedThisSecond = new AtomicInteger(0);
    private static final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private static final AtomicInteger errorCount = new AtomicInteger(0);
    private static ExecutorService executor;
    private static final int BATCH_SIZE = 1000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
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
            writePolicy.totalTimeout = 500; // 500ms timeout

            // Initialize Kafka consumer
            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(BATCH_SIZE));
            kafkaProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800"); // 50MB
            kafkaProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576"); // 1MB
            kafkaProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
            kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

            kafkaConsumer = new KafkaConsumer<>(kafkaProps);
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            // Initialize thread pool
            ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                workerPoolSize,
                workerPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executor = customExecutor;

            // Create RateLimiter
            RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond + 1000);

            // Schedule metrics reporting
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                int currentRate = messagesProcessedThisSecond.getAndSet(0);
                long total = totalMessagesProcessed.get();
                int errors = errorCount.get();
                System.out.printf("Messages ---> Processed: %d/s, Total: %d, Errors: %d, Active threads: %d%n",
                    currentRate, total, errors, customExecutor.getActiveCount());
            }, 0, 1, TimeUnit.SECONDS);

            // Start processing
            processMessages(kafkaConsumer, destinationClient, writePolicy, 
                          destinationNamespace, setName, rateLimiter);

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
                                     RateLimiter rateLimiter) {
        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) continue;

                CountDownLatch latch = new CountDownLatch(records.count());
                List<Future<?>> futures = new ArrayList<>(records.count());

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    rateLimiter.acquire();

                    Future<?> future = executor.submit(() -> {
                        try {
                            processRecord(record, destinationClient, writePolicy, 
                                        destinationNamespace, setName);
                            messagesProcessedThisSecond.incrementAndGet();
                            totalMessagesProcessed.incrementAndGet();
                        } catch (Exception e) {
                            System.err.println("Error processing record: " + e.getMessage());
                            errorCount.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    });
                    futures.add(future);
                }

                // Wait for all records to be processed
                latch.await(5, TimeUnit.SECONDS);
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
            String userId = new String(record.key());
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
                if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
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
