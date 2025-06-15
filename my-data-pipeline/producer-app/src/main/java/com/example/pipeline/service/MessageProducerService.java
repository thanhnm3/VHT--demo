package com.example.pipeline.service;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.example.pipeline.service.config.ConfigProducerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.util.*;
import java.util.concurrent.*;
import java.util.Base64;


public class MessageProducerService {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducerService.class);
    private final Map<String, String> regionToTopicMap;
    private final Queue<ProducerRecord<byte[], byte[]>> pendingMessages;
    private final Object pendingMessagesLock;
    private final ObjectMapper objectMapper;
    private final ConfigProducerService configService;

    public MessageProducerService() {
        this.regionToTopicMap = new ConcurrentHashMap<>();
        this.pendingMessages = new ConcurrentLinkedQueue<>();
        this.pendingMessagesLock = new Object();
        this.objectMapper = new ObjectMapper();
        this.configService = ConfigProducerService.getInstance();
    }

    public void initializeTopicMapping(Map<String, String> topicMapping) {
        // Sử dụng TopicGenerator để tạo mapping cho CDC topics
        Map<String, String> generatedTopics = TopicGenerator.generateCdcTopics();
        this.regionToTopicMap.putAll(generatedTopics);
        // Chỉ log một lần khi khởi tạo mapping
        if (logger.isDebugEnabled()) {
            logger.debug("Initialized region to topic mapping: {}", regionToTopicMap);
        }
    }

    public boolean isValidRecord(Record record) {
        return record != null && record.bins != null && !record.bins.isEmpty();
    }

    public String getRegionOfProvince(String province) {
        return configService.getRegionOfProvince(province);
    }

    public List<String> getConsumersForRegion(String region) {
        try {
            return configService.getConsumersForRegion(region);
        } catch (Exception e) {
            logger.warn("Error getting consumers for region {}: {}", region, e.getMessage());
            return null;
        }
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

        // Lấy region từ record
        String region = (String) record.getValue("region");
        String province = (String) record.getValue("province");
        
        // Nếu không có region, lấy từ province
        if (region == null && province != null) {
            region = getRegionOfProvince(province);
        }

        if (region == null) {
            logSkippedMessage(new String(keyBytes), "No region found in record");
            return null;
        }

        // Lấy topic từ mapping
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
            String userId = (String) record.getValue("user_id");
            String phone = (String) record.getValue("phone");
            String serviceType = (String) record.getValue("service_type");
            String province = (String) record.getValue("province");
            String region = (String) record.getValue("region");
            Object lastUpdated = record.getValue("last_updated");
            Object notes = record.getValue("notes");

            // Kiểm tra và lấy region từ province nếu region không có
            if (region == null && province != null) {
                region = getRegionOfProvince(province);
            }

            // Thêm các trường vào data map
            data.put("user_id", userId);
            data.put("phone", phone);
            data.put("service_type", serviceType);
            data.put("province", province);
            data.put("region", region);
            
            // Xử lý last_updated
            long lastUpdateTimestamp;
            if (lastUpdated != null) {
                if (lastUpdated instanceof Long) {
                    lastUpdateTimestamp = (Long) lastUpdated;
                } else if (lastUpdated instanceof Integer) {
                    lastUpdateTimestamp = ((Integer) lastUpdated).longValue();
                } else {
                    lastUpdateTimestamp = System.currentTimeMillis();
                }
            } else {
                lastUpdateTimestamp = System.currentTimeMillis();
            }
            data.put("last_updated", lastUpdateTimestamp);

            // Xử lý notes
            if (notes != null) {
                if (notes instanceof byte[]) {
                    // Nếu là byte array, chuyển thành Base64 string
                    data.put("notes", Base64.getEncoder().encodeToString((byte[]) notes));
                } else {
                    // Nếu không phải byte array, giữ nguyên giá trị
                    data.put("notes", notes);
                }
            }

            // Thêm thông tin về consumer groups cho region này (nếu có)
            if (region != null) {
                try {
                    List<String> consumers = getConsumersForRegion(region);
                    if (consumers != null && !consumers.isEmpty()) {
                        data.put("target_consumers", consumers);
                    }
                } catch (Exception e) {
                    // Log warning nhưng vẫn tiếp tục xử lý
                    logger.warn("Could not get consumers for region {}: {}", region, e.getMessage());
                }
            }

            // Cấu hình ObjectMapper để xử lý byte array
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            logger.error("Error serializing record: {}", e.getMessage(), e);
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
        logger.warn("Skipped message with key {}: {}", key, reason);
    }

    public void logFailedMessage(ProducerRecord<byte[], byte[]> record, String reason, Exception e) {
        logger.error("Failed to process message: {} - {}", reason, e.getMessage());
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
