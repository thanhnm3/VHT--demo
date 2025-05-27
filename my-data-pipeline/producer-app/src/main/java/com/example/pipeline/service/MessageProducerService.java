package com.example.pipeline.service;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MessageProducerService {
    private final AtomicLong producedCount;
    private final AtomicLong failedMessages;
    private final AtomicLong skippedMessages;
    private final Map<String, String> prefixToTopicMap;
    private String defaultTopic;
    private final Queue<ProducerRecord<byte[], byte[]>> pendingMessages;
    private final Object pendingMessagesLock;

    public MessageProducerService() {
        this.producedCount = new AtomicLong(0);
        this.failedMessages = new AtomicLong(0);
        this.skippedMessages = new AtomicLong(0);
        this.prefixToTopicMap = new ConcurrentHashMap<>();
        this.defaultTopic = "";
        this.pendingMessages = new ConcurrentLinkedQueue<>();
        this.pendingMessagesLock = new Object();
    }

    public void initializeTopicMapping(Map<String, String> topicMapping, String defaultTopic) {
        this.prefixToTopicMap.putAll(topicMapping);
        this.defaultTopic = defaultTopic;
    }

    public boolean isValidRecord(Record record) {
        return record != null && record.bins != null;
    }

    public ProducerRecord<byte[], byte[]> createKafkaRecord(Key key, Record record) {
        byte[] keyBytes = (byte[]) key.userKey.getObject();
        String message;

        if (record != null && record.bins != null) {
            byte[] personData = (byte[]) record.getValue("personData");
            long lastUpdate = record.getValue("lastUpdate") != null ? 
                            (long) record.getValue("lastUpdate") : 
                            System.currentTimeMillis();

            message = String.format("{\"personData\": %s, \"lastUpdate\": %d}",
                    personData != null ? "\"" + Base64.getEncoder().encodeToString(personData) + "\"" : "null",
                    lastUpdate);
        } else {
            message = String.format("{\"personData\": null, \"lastUpdate\": %d}", 
                    System.currentTimeMillis());
        }

        String topic = determineTopic(keyBytes);
        return new ProducerRecord<>(
                topic,
                keyBytes,
                message.getBytes(StandardCharsets.UTF_8)
        );
    }

    private String determineTopic(byte[] key) {
        if (key == null || key.length < 3) {
            return defaultTopic;
        }
        
        String prefix = new String(key, 0, 3);
        return prefixToTopicMap.getOrDefault(prefix, defaultTopic);
    }

    public void sendBatch(KafkaProducer<byte[], byte[]> producer, 
                         List<ProducerRecord<byte[], byte[]>> batch, 
                         int maxRetries) {
        for (ProducerRecord<byte[], byte[]> record : batch) {
            try {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logFailedMessage(record, "Failed to send message", exception);
                    } else {
                        producedCount.incrementAndGet();
                    }
                });
            } catch (Exception e) {
                logFailedMessage(record, "Error sending message", e);
            }
        }
    }

    public void logSkippedMessage(String key, String reason) {
        System.out.printf("Skipped message with key %s: %s%n", key, reason);
        skippedMessages.incrementAndGet();
    }

    public void logFailedMessage(ProducerRecord<byte[], byte[]> record, String reason, Throwable e) {
        System.err.printf("Failed to process message: %s. Reason: %s%n", 
                         new String(record.key()), reason);
        if (e != null) {
            e.printStackTrace();
        }
        failedMessages.incrementAndGet();
    }

    public void addPendingMessage(ProducerRecord<byte[], byte[]> record) {
        synchronized (pendingMessagesLock) {
            pendingMessages.offer(record);
        }
    }

    public boolean hasPendingProducerMessages() {
        synchronized (pendingMessagesLock) {
            return !pendingMessages.isEmpty();
        }
    }

    public void processPendingProducerMessages(KafkaProducer<byte[], byte[]> producer, int maxRetries) {
        synchronized (pendingMessagesLock) {
            while (!pendingMessages.isEmpty()) {
                ProducerRecord<byte[], byte[]> record = pendingMessages.poll();
                if (record != null) {
                    try {
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                logFailedMessage(record, "Failed to send pending message", exception);
                            } else {
                                producedCount.incrementAndGet();
                            }
                        });
                    } catch (Exception e) {
                        logFailedMessage(record, "Error sending pending message", e);
                    }
                }
            }
        }
    }

    public void printMessageStats() {
        System.out.println("\nMessage Processing Statistics:");
        System.out.printf("Total messages produced: %d%n", producedCount.get());
        System.out.printf("Failed messages: %d%n", failedMessages.get());
        System.out.printf("Skipped messages: %d%n", skippedMessages.get());
    }

    public long getProducedCount() {
        return producedCount.get();
    }

    public long getFailedMessages() {
        return failedMessages.get();
    }

    public long getSkippedMessages() {
        return skippedMessages.get();
    }
}
