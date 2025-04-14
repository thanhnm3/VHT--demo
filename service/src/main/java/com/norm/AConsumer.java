package com.norm;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collections;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AConsumer {

    // Load configuration from .env
    private static final Dotenv dotenv = Dotenv.configure().directory("service//.env").load();

    // Aerospike configuration
    private static final String AEROSPIKE_HOST = dotenv.get("AEROSPIKE_CONSUMER_HOST");
    private static final int AEROSPIKE_PORT = Integer.parseInt(dotenv.get("AEROSPIKE_CONSUMER_PORT"));
    private static final String NAMESPACE = dotenv.get("CONSUMER_NAMESPACE");
    private static final String SET_NAME = dotenv.get("CONSUMER_SET_NAME");

    // Kafka configuration
    private static final String KAFKA_BROKER = dotenv.get("KAFKA_BROKER");
    private static final String KAFKA_TOPIC = dotenv.get("KAFKA_TOPIC");
    private static final String GROUP_ID = dotenv.get("CONSUMER_GROUP");

    private static final AtomicInteger messagesProcessedThisSecond = new AtomicInteger(0);
    private static ExecutorService executor;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond) {
        executor = Executors.newFixedThreadPool(workerPoolSize);
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AerospikeClient aerospikeClient = null;

        try {
            aerospikeClient = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);
            WritePolicy writePolicy = new WritePolicy();

            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            kafkaConsumer = new KafkaConsumer<>(kafkaProps);
            kafkaConsumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

            final AerospikeClient finalAerospikeClient = aerospikeClient;
            final WritePolicy finalWritePolicy = writePolicy;

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages processed in the last second: " + messagesProcessedThisSecond.get());
                messagesProcessedThisSecond.set(0);
            }, 0, 1, TimeUnit.SECONDS);

            while (true) {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    executor.submit(() -> {
                        try {
                            while (messagesProcessedThisSecond.get() >= maxMessagesPerSecond) {
                                Thread.sleep(1);
                            }
                            processRecord(finalAerospikeClient, finalWritePolicy, record);
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

    private static void processRecord(AerospikeClient aerospikeClient, WritePolicy writePolicy, ConsumerRecord<byte[], byte[]> record) {
        // Xử lý record và ghi vào Aerospike
        try {
            byte[] keyBytes = record.key();
            byte[] value = record.value();

            if (keyBytes == null || value == null) {
                System.err.println("Received null key or value, skipping record.");
                return;
            }

            String keyString = new String(keyBytes);
            Key aerospikeKey = new Key(NAMESPACE, SET_NAME, keyString);
            Bin dataBin = new Bin("data", value);
            Bin PKBin = new Bin("PK", keyString);

            // Write data to Aerospike
            aerospikeClient.put(writePolicy, aerospikeKey, PKBin, dataBin);
        } catch (Exception e) {
            System.err.println("Failed to process record: " + e.getMessage());
            e.printStackTrace();
        }
    }
}