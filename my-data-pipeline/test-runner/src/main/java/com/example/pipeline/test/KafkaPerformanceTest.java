package com.example.pipeline.test;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaPerformanceTest {
    private static final String TOPIC = "performance-test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    private static final int MIN_MESSAGE_SIZE = 100; // 100B
    private static final int MAX_MESSAGE_SIZE = 2048; // 2KB
    private static final int NUM_MESSAGES = 1000000; // 1 million messages
    private static final int NUM_CONSUMERS = 3;
    private static final int NUM_PRODUCERS = 3;
    private static final Random random = new Random();

    public static void main(String[] args) throws InterruptedException {
        // Run producers
        System.out.println("Starting producers...");
        CountDownLatch producerLatch = new CountDownLatch(NUM_PRODUCERS);
        ExecutorService producerExecutor = Executors.newFixedThreadPool(NUM_PRODUCERS);
        AtomicLong totalProduced = new AtomicLong(0);
        AtomicLong totalBytesProduced = new AtomicLong(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_PRODUCERS; i++) {
            producerExecutor.submit(() -> {
                try {
                    Producer<String, String> producer = createProducer();
                    long produced = 0;
                    long bytesProduced = 0;
                    for (int j = 0; j < NUM_MESSAGES / NUM_PRODUCERS; j++) {
                        String message = generateRandomMessage();
                        producer.send(new ProducerRecord<>(TOPIC, message));
                        produced++;
                        bytesProduced += message.getBytes().length;
                    }
                    producer.flush();
                    producer.close();
                    totalProduced.addAndGet(produced);
                    totalBytesProduced.addAndGet(bytesProduced);
                } finally {
                    producerLatch.countDown();
                }
            });
        }

        producerLatch.await();
        long producerEndTime = System.currentTimeMillis();
        long producerDuration = producerEndTime - startTime;
        System.out.printf("Produced %d messages (%.2f MB) in %d ms%n", 
            totalProduced.get(), 
            totalBytesProduced.get() / (1024.0 * 1024.0),
            producerDuration);
        System.out.printf("Throughput: %.2f msgs/sec, %.2f MB/sec%n", 
            (totalProduced.get() * 1000.0) / producerDuration,
            (totalBytesProduced.get() * 1000.0) / (1024.0 * 1024.0 * producerDuration));

        // Run consumers
        System.out.println("\nStarting consumers...");
        CountDownLatch consumerLatch = new CountDownLatch(NUM_CONSUMERS);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMERS);
        AtomicLong totalConsumed = new AtomicLong(0);
        AtomicLong totalBytesConsumed = new AtomicLong(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_CONSUMERS; i++) {
            consumerExecutor.submit(() -> {
                try {
                    Consumer<String, String> consumer = createConsumer();
                    consumer.subscribe(Collections.singletonList(TOPIC));
                    long consumed = 0;
                    long bytesConsumed = 0;
                    while (consumed < NUM_MESSAGES / NUM_CONSUMERS) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        for (ConsumerRecord<String, String> record : records) {
                            consumed++;
                            bytesConsumed += record.value().getBytes().length;
                        }
                    }
                    consumer.close();
                    totalConsumed.addAndGet(consumed);
                    totalBytesConsumed.addAndGet(bytesConsumed);
                } finally {
                    consumerLatch.countDown();
                }
            });
        }

        consumerLatch.await();
        long consumerEndTime = System.currentTimeMillis();
        long consumerDuration = consumerEndTime - startTime;
        System.out.printf("Consumed %d messages (%.2f MB) in %d ms%n", 
            totalConsumed.get(), 
            totalBytesConsumed.get() / (1024.0 * 1024.0),
            consumerDuration);
        System.out.printf("Throughput: %.2f msgs/sec, %.2f MB/sec%n", 
            (totalConsumed.get() * 1000.0) / consumerDuration,
            (totalBytesConsumed.get() * 1000.0) / (1024.0 * 1024.0 * consumerDuration));

        producerExecutor.shutdown();
        consumerExecutor.shutdown();
    }

    private static String generateRandomMessage() {
        int size = MIN_MESSAGE_SIZE + random.nextInt(MAX_MESSAGE_SIZE - MIN_MESSAGE_SIZE + 1);
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return new KafkaProducer<>(props);
    }

    private static Consumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "performance-test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        return new KafkaConsumer<>(props);
    }
} 