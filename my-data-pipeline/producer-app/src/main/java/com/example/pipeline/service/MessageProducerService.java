package com.example.pipeline.service;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MessageProducerService {
    private final Map<String, String> regionToTopicMap;
    private final Queue<ProducerRecord<byte[], byte[]>> pendingMessages;
    private final ObjectMapper objectMapper;
    private final AtomicLong pendingMessageCount = new AtomicLong(0);

    public MessageProducerService() {
        this.regionToTopicMap = new ConcurrentHashMap<>();
        this.pendingMessages = new ConcurrentLinkedQueue<>();
        this.objectMapper = new ObjectMapper();
    }

    public void initializeTopicMapping(Map<String, String> topicMapping) {
        this.regionToTopicMap.putAll(topicMapping);
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

        String region = (String) record.getValue("region");
        if (region == null) {
            logSkippedMessage(new String(keyBytes), "Region field is null or invalid");
            return null;
        }

        String topic = regionToTopicMap.get(region);
        if (topic == null) {
            logSkippedMessage(new String(keyBytes), "No topic mapping found for region: " + region);
            return null;
        }

        byte[] valueBytes = serializeRecord(record);
        if (valueBytes == null) {
            logSkippedMessage(new String(keyBytes), "Failed to serialize record");
            return null;
        }

        return new ProducerRecord<>(topic, keyBytes, valueBytes);
    }


    private byte[] serializeRecord(Record record) {
        try {
            Map<String, Object> data = new HashMap<>();
            
            // Lấy các trường dữ liệu từ record
            data.put("user_id", record.getValue("user_id"));
            data.put("phone", record.getValue("phone"));
            data.put("service_type", record.getValue("service_type"));
            data.put("province", record.getValue("province"));
            data.put("region", record.getValue("region"));
            
            // Xử lý last_updated
            Object lastUpdateObj = record.getValue("last_updated");
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
            data.put("last_updated", lastUpdate);

            // Giữ nguyên trường notes
            data.put("notes", record.getValue("notes"));

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

    public void addPendingMessage(ProducerRecord<byte[], byte[]> record) {
        pendingMessages.add(record);
        pendingMessageCount.incrementAndGet();
    }

    public void processPendingProducerMessages(KafkaProducer<byte[], byte[]> producer, int maxRetries) {
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>();
        ProducerRecord<byte[], byte[]> record;
        
        while ((record = pendingMessages.poll()) != null) {
            batch.add(record);
            pendingMessageCount.decrementAndGet();
            
            if (batch.size() >= 100) {
                sendBatch(producer, batch, maxRetries);
                batch.clear();
            }
        }
        
        if (!batch.isEmpty()) {
            sendBatch(producer, batch, maxRetries);
        }
    }

    public long getPendingMessageCount() {
        return pendingMessageCount.get();
    }
}
