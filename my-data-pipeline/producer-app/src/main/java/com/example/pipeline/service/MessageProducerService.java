package com.example.pipeline.service;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.Base64;

public class MessageProducerService {
    private final Map<String, String> prefixToTopicMap;
    private final Queue<ProducerRecord<byte[], byte[]>> pendingMessages;
    private final Object pendingMessagesLock;
    private final ObjectMapper objectMapper;

    public MessageProducerService() {
        this.prefixToTopicMap = new ConcurrentHashMap<>();
        this.pendingMessages = new ConcurrentLinkedQueue<>();
        this.pendingMessagesLock = new Object();
        this.objectMapper = new ObjectMapper();
    }

    public void initializeTopicMapping(Map<String, String> topicMapping) {
        this.prefixToTopicMap.putAll(topicMapping);
    }

    public boolean isValidRecord(Record record) {
        return record != null && record.bins != null;
    }

    public ProducerRecord<byte[], byte[]> createKafkaRecord(Key key, Record record) {
        if (key == null || record == null) {
            return null;
        }

        byte[] keyBytes = (byte[]) key.userKey.getObject();
        if (keyBytes == null) {
            logSkippedMessage("null", "Key is null");
            return null;
        }

        String prefix = extractPrefix(keyBytes);
        if (prefix == null) {
            logSkippedMessage(new String(keyBytes), "Invalid key format");
            return null;
        }

        String topic = prefixToTopicMap.get(prefix);
        if (topic == null) {
            logSkippedMessage(new String(keyBytes), "No topic mapping found for prefix: " + prefix);
            return null;
        }

        byte[] valueBytes = serializeRecord(record);
        if (valueBytes == null) {
            logSkippedMessage(new String(keyBytes), "Failed to serialize record");
            return null;
        }

        return new ProducerRecord<>(topic, keyBytes, valueBytes);
    }

    private String extractPrefix(byte[] key) {
        // Extract prefix from key (e.g., "096123456" -> "096")
        if (key == null || key.length < 3) {
            return null;
        }
        // Convert first 3 bytes to string
        return new String(key, 0, 3, StandardCharsets.UTF_8);
    }

    private byte[] serializeRecord(Record record) {
        try {
            Map<String, Object> data = new HashMap<>();
            
            // Lấy personData dạng byte array
            byte[] personData = (byte[]) record.getValue("personData");
            if (personData != null) {
                // Encode byte array thành base64 string để JSON có thể serialize
                String personDataBase64 = Base64.getEncoder().encodeToString(personData);
                data.put("personData", personDataBase64);
            } else {
                data.put("personData", null);
            }

            // Lấy lastUpdate dạng timestamp
            Object lastUpdateObj = record.getValue("lastUpdate");
            long lastUpdate;
            if (lastUpdateObj != null) {
                if (lastUpdateObj instanceof Long) {
                    lastUpdate = (Long) lastUpdateObj;
                } else if (lastUpdateObj instanceof Integer) {
                    lastUpdate = ((Integer) lastUpdateObj).longValue();
                } else {
                    lastUpdate = System.currentTimeMillis();
                }
            } else {
                lastUpdate = System.currentTimeMillis();
            }
            data.put("lastUpdate", lastUpdate);

            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            logSkippedMessage(record.toString(), "Failed to serialize record: " + e.getMessage());
            return null;
        }
    }

    public void sendBatch(KafkaProducer<byte[], byte[]> producer, List<ProducerRecord<byte[], byte[]>> batch, int maxRetries) {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        for (ProducerRecord<byte[], byte[]> record : batch) {
            if (record == null) continue;

            int retries = 0;
            boolean sent = false;

            while (!sent && retries < maxRetries) {
                try {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logFailedMessage(record, "Failed to send message", exception);
                        }
                    });
                    sent = true;
                } catch (Exception e) {
                    retries++;
                    if (retries >= maxRetries) {
                        logFailedMessage(record, "Failed to send message after " + maxRetries + " retries", e);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        // Ensure all messages in the batch are sent
        producer.flush();
    }

    public void logSkippedMessage(String key, String reason) {
        System.err.printf("Skipped message with key %s: %s%n", key, reason);
    }

    public void logFailedMessage(ProducerRecord<byte[], byte[]> record, String reason, Throwable e) {
        String key = record.key() != null ? new String(record.key(), StandardCharsets.UTF_8) : "null";
        System.err.printf("Failed to process message with key %s: %s%n", key, reason);
        if (e != null) {
            e.printStackTrace();
        }
    }

    public boolean hasPendingProducerMessages() {
        return !pendingMessages.isEmpty();
    }

    public void processPendingProducerMessages(KafkaProducer<byte[], byte[]> producer, int maxRetries) {
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>();
        synchronized (pendingMessagesLock) {
            while (!pendingMessages.isEmpty()) {
                batch.add(pendingMessages.poll());
            }
        }
        if (!batch.isEmpty()) {
            sendBatch(producer, batch, maxRetries);
        }
    }
}
