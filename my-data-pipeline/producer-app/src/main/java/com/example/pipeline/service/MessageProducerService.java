package com.example.pipeline.service;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.example.pipeline.service.config.ConfigProducerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.example.pipeline.proto.ProtoSubscriberInfo;
import com.example.pipeline.proto.ProtoSubscriber;
import com.example.pipeline.proto.ProtoBalance;
import com.example.pipeline.proto.ProtoAcmBalance;
import com.example.pipeline.proto.ProtoProduct;
import com.example.pipeline.proto.ProtoCharacteristic;
import com.example.pipeline.proto.ProtoHistory;
import com.google.protobuf.Message;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MessageProducerService {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducerService.class);
    private final Map<String, String> regionToTopicMap;
    private final Queue<ProducerRecord<byte[], byte[]>> pendingMessages;
    private final Object pendingMessagesLock;
    private final ObjectMapper objectMapper;
    private final ConfigProducerService configService;
    private final AtomicLong totalMessagesSent = new AtomicLong(0);
    private final AtomicLong messagesInLastSecond = new AtomicLong(0);
    private final AtomicLong totalMessagesSkipped = new AtomicLong(0);
    private final AtomicLong totalMessagesFailed = new AtomicLong(0);
    private final ScheduledExecutorService messageRateMonitor;

    public MessageProducerService() {
        this.regionToTopicMap = new ConcurrentHashMap<>();
        this.pendingMessages = new ConcurrentLinkedQueue<>();
        this.pendingMessagesLock = new Object();
        this.objectMapper = new ObjectMapper();
        this.configService = ConfigProducerService.getInstance();
        
        // Initialize message rate monitor with simplified logging
        this.messageRateMonitor = Executors.newSingleThreadScheduledExecutor();
        this.messageRateMonitor.scheduleAtFixedRate(() -> {
            long currentCount = messagesInLastSecond.getAndSet(0);
            if (currentCount > 0) {
                logger.info("[Message Rate] {} msg/sec", currentCount);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void incrementMessageCounters() {
        totalMessagesSent.incrementAndGet();
        messagesInLastSecond.incrementAndGet();
    }

    public void shutdown(KafkaProducer<byte[], byte[]> producer) {
        if (producer != null) {
            try {
                // Process any pending messages
                processPendingProducerMessages(producer, 3);
                
                // Flush all buffered messages
                producer.flush();
                
                // Log final message rate before shutdown
                long finalCount = messagesInLastSecond.get();
                if (finalCount > 0) {
                    logger.info("[Message Rate] {} msg/sec (final)", finalCount);
                }
            } catch (Exception e) {
                logger.error("Error during producer shutdown: {}", e.getMessage());
            }
        }

        if (messageRateMonitor != null) {
            messageRateMonitor.shutdown();
            try {
                if (!messageRateMonitor.awaitTermination(1, TimeUnit.MINUTES)) {
                    messageRateMonitor.shutdownNow();
                }
            } catch (InterruptedException e) {
                messageRateMonitor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void initializeTopicMapping(Map<String, String> topicMapping) {
        // Sử dụng TopicGenerator để tạo mapping cho A topics
        Map<String, String> generatedTopics = TopicGenerator.generateTopics();
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
        try {
            Object subValue = record.getValue("sub");
            if (!(subValue instanceof Map)) {
                logger.warn("[Validation] Invalid subscriber data format for key: {}", key.userKey);
                totalMessagesSkipped.incrementAndGet();
                return null;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> subscriberData = (Map<String, Object>) subValue;

            // Validate subscriber data
            if (!validateSubscriberData(subscriberData, key.userKey.toString())) {
                return null;
            }

            String region = (String) subscriberData.get("r");
            if (region == null) {
                logger.warn("[Validation] No region found for key: {}", key.userKey);
                totalMessagesSkipped.incrementAndGet();
                return null;
            }

            String topic = regionToTopicMap.get(region);
            if (topic == null) {
                logger.warn("[Validation] No topic found for region: {}", region);
                totalMessagesSkipped.incrementAndGet();
                return null;
            }

            // Create ProtoSubscriberInfo
            ProtoSubscriberInfo.Builder builder = ProtoSubscriberInfo.newBuilder();
            
            // Set subscriber info
            ProtoSubscriber subscriber = ProtoSubscriber.newBuilder()
                .setMsisdn(getStringValue(subscriberData, "m"))
                .setSubId(getLongValue(subscriberData, "si"))
                .setCustId(getLongValue(subscriberData, "ci"))
                .setCustType(getStringValue(subscriberData, "ct"))
                .setRegion(getStringValue(subscriberData, "r"))
                .setSubType(getIntValue(subscriberData, "st"))
                .setStateSet(getStringValue(subscriberData, "ss"))
                .setLastUpdate(getLongValue(subscriberData, "lu"))
                .build();
            builder.setSubscriber(subscriber);

            // Process additional data
            processBalanceData(record, builder);
            processAcmBalanceData(record, builder);
            processProductData(record, builder);
            processCharacteristicData(record, builder);
            processHistoryData(record, builder);

            // Set generation
            builder.setGeneration(record.generation);

            // Build and serialize Proto message
            ProtoSubscriberInfo protoMessage = builder.build();
            byte[] value = protoMessage.toByteArray();

            return new ProducerRecord<>(topic, key.userKey.toString().getBytes(), value);
        } catch (Exception e) {
            logger.error("[Processing Error] Failed to create Kafka record: {}", e.getMessage());
            totalMessagesFailed.incrementAndGet();
            return null;
        }
    }

    private boolean validateSubscriberData(Map<String, Object> subscriberData, String key) {
        // Check required fields
        String[] requiredFields = {"m", "si", "ci", "ct", "rt", "ss", "st", "lu"};
        for (String field : requiredFields) {
            if (!subscriberData.containsKey(field)) {
                logSkippedMessage(key, "Missing required field: " + field);
                logger.debug("[Validation] Missing field {} in subscriber data. Available fields: {}", 
                    field, subscriberData.keySet());
                return false;
            }
        }

        // Validate field types
        try {
            // Validate MSISDN
            Object msisdn = subscriberData.get("m");
            if (!(msisdn instanceof String) || ((String) msisdn).isEmpty()) {
                logSkippedMessage(key, "Invalid MSISDN format: " + (msisdn != null ? msisdn.getClass().getName() : "null"));
                return false;
            }

            // Validate numeric fields
            String[] numericFields = {"si", "ci", "st", "lu"};
            for (String field : numericFields) {
                Object value = subscriberData.get(field);
                if (!(value instanceof Number)) {
                    logSkippedMessage(key, "Invalid numeric field: " + field + ", value type: " + 
                        (value != null ? value.getClass().getName() : "null"));
                    return false;
                }
            }

            // Validate string fields
            String[] stringFields = {"ct", "rt", "ss"};
            for (String field : stringFields) {
                Object value = subscriberData.get(field);
                if (!(value instanceof String) || ((String) value).isEmpty()) {
                    logSkippedMessage(key, "Invalid string field: " + field + ", value type: " + 
                        (value != null ? value.getClass().getName() : "null"));
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            logSkippedMessage(key, "Error validating subscriber data: " + e.getMessage());
            logger.error("[Validation] Exception during validation: ", e);
            return false;
        }
    }

    private byte[] serializeRecord(Record record) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            // Cấu hình Jackson để bỏ qua các trường gây ra tham chiếu vòng tròn
            mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            mapper.configure(SerializationFeature.WRITE_SELF_REFERENCES_AS_NULL, true);
            
            // Tạo một Map để chứa dữ liệu cần serialize
            Map<String, Object> dataToSerialize = new HashMap<>();
            
            // Lấy dữ liệu từ các bin
            for (Map.Entry<String, Object> entry : record.bins.entrySet()) {
                String binName = entry.getKey();
                Object value = entry.getValue();
                
                // Nếu là đối tượng Protocol Buffer, chuyển đổi thành Map
                if (value instanceof Message) {
                    Message protoMessage = (Message) value;
                    Map<String, Object> protoMap = new HashMap<>();
                    protoMessage.getAllFields().forEach((field, fieldValue) -> {
                        protoMap.put(field.getName(), fieldValue);
                    });
                    dataToSerialize.put(binName, protoMap);
                } else {
                    dataToSerialize.put(binName, value);
                }
            }
            
            // Thêm generation
            dataToSerialize.put("generation", record.generation);
            
            return mapper.writeValueAsBytes(dataToSerialize);
        } catch (Exception e) {
            logger.error("[Serialization Error] Failed to serialize record: {}", e.getMessage(), e);
            return null;
        }
    }

    private String getStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString() : "";
    }

    private long getLongValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0L;
    }

    private int getIntValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
    }


    private void processBalanceData(Record record, ProtoSubscriberInfo.Builder builder) {
        Object balValue = record.getValue("bal");
        if (balValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> balanceData = (Map<String, Map<String, Object>>) balValue;
            for (Map.Entry<String, Map<String, Object>> entry : balanceData.entrySet()) {
                try {
                    Map<String, Object> bal = entry.getValue();
                    ProtoBalance balance = ProtoBalance.newBuilder()
                        .setId(getLongValue(bal, "i"))
                        .setGross(getLongValue(bal, "g"))
                        .setConsume(getLongValue(bal, "c"))
                        .setReserve(getLongValue(bal, "r"))
                        .setBalType(getLongValue(bal, "t"))
                        .setQuotaMax(getLongValue(bal, "q"))
                        .setRecurringDay(getLongValue(bal, "d"))
                        .setOwnerValue(getStringValue(bal, "o"))
                        .setEffDate(getLongValue(bal, "e"))
                        .setExpDate(getLongValue(bal, "x"))
                        .setUpdateDate(getLongValue(bal, "u"))
                        .setState(getIntValue(bal, "s"))
                        .setLevel(getIntValue(bal, "v"))
                        .setOf(getLongValue(bal, "f"))
                        .build();
                    builder.putBalance(Long.parseLong(entry.getKey().substring(1)), balance);
                } catch (Exception e) {
                    logger.warn("[Balance Processing] Error processing balance entry {}: {}", 
                        entry.getKey(), e.getMessage());
                }
            }
        }
    }

    private void processAcmBalanceData(Record record, ProtoSubscriberInfo.Builder builder) {
        Object acmValue = record.getValue("acm");
        if (acmValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> acmBalanceData = (Map<String, Map<String, Object>>) acmValue;
            for (Map.Entry<String, Map<String, Object>> entry : acmBalanceData.entrySet()) {
                try {
                    Map<String, Object> acmBal = entry.getValue();
                    ProtoAcmBalance acmBalance = ProtoAcmBalance.newBuilder()
                        .setId(getLongValue(acmBal, "i"))
                        .setValue(getLongValue(acmBal, "v"))
                        .setReserve(getLongValue(acmBal, "r"))
                        .setBalType(getLongValue(acmBal, "t"))
                        .setBillingCycleId(getLongValue(acmBal, "b"))
                        .setLimit(getLongValue(acmBal, "l"))
                        .setEffDate(getLongValue(acmBal, "e"))
                        .setExpDate(getLongValue(acmBal, "x"))
                        .setUpdateDate(getLongValue(acmBal, "u"))
                        .setState(getIntValue(acmBal, "s"))
                        .setLevel(getIntValue(acmBal, "l"))
                        .setOf(getLongValue(acmBal, "f"))
                        .build();
                    builder.putAcmBalance(Long.parseLong(entry.getKey().substring(1)), acmBalance);
                } catch (Exception e) {
                    logger.warn("[ACM Balance Processing] Error processing ACM balance entry {}: {}", 
                        entry.getKey(), e.getMessage());
                }
            }
        }
    }

    private void processProductData(Record record, ProtoSubscriberInfo.Builder builder) {
        Object prdValue = record.getValue("prd");
        if (prdValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> productData = (Map<String, Map<String, Object>>) prdValue;
            for (Map.Entry<String, Map<String, Object>> entry : productData.entrySet()) {
                try {
                    Map<String, Object> prod = entry.getValue();
                    ProtoProduct product = ProtoProduct.newBuilder()
                        .setId(getLongValue(prod, "i"))
                        .setProductOfferingId(getLongValue(prod, "o"))
                        .setRecurringDay(getLongValue(prod, "d"))
                        .setEffDate(getLongValue(prod, "e"))
                        .setExpDate(getLongValue(prod, "x"))
                        .setUpdateDate(getLongValue(prod, "u"))
                        .setState(getIntValue(prod, "s"))
                        .setLevel(getIntValue(prod, "l"))
                        .setOf(getLongValue(prod, "f"))
                        .build();
                    builder.putProduct(Long.parseLong(entry.getKey().substring(1)), product);
                } catch (Exception e) {
                    logger.warn("[Product Processing] Error processing product entry {}: {}", 
                        entry.getKey(), e.getMessage());
                }
            }
        }
    }

    private void processCharacteristicData(Record record, ProtoSubscriberInfo.Builder builder) {
        Object chrValue = record.getValue("chr");
        if (chrValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> charData = (Map<String, Map<String, Object>>) chrValue;
            for (Map.Entry<String, Map<String, Object>> entry : charData.entrySet()) {
                try {
                    Map<String, Object> charac = entry.getValue();
                    ProtoCharacteristic characteristic = ProtoCharacteristic.newBuilder()
                        .setId(getLongValue(charac, "i"))
                        .setCharSpecId(getLongValue(charac, "s"))
                        .setBillingCycleId(getLongValue(charac, "b"))
                        .setValue(getStringValue(charac, "v"))
                        .setLongValue(getLongValue(charac, "l"))
                        .setEffDate(getLongValue(charac, "e"))
                        .setExpDate(getLongValue(charac, "x"))
                        .setUpdateDate(getLongValue(charac, "u"))
                        .setState(getIntValue(charac, "s"))
                        .setLevel(getIntValue(charac, "l"))
                        .setOf(getLongValue(charac, "f"))
                        .build();
                    builder.putCharacteristic(Long.parseLong(entry.getKey().substring(1)), characteristic);
                } catch (Exception e) {
                    logger.warn("[Characteristic Processing] Error processing characteristic entry {}: {}", 
                        entry.getKey(), e.getMessage());
                }
            }
        }
    }

    private void processHistoryData(Record record, ProtoSubscriberInfo.Builder builder) {
        Object hisValue = record.getValue("his");
        if (hisValue instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Map<String, Object>> historyData = (Map<String, Map<String, Object>>) hisValue;
            for (Map.Entry<String, Map<String, Object>> entry : historyData.entrySet()) {
                try {
                    Map<String, Object> hist = entry.getValue();
                    ProtoHistory history = ProtoHistory.newBuilder()
                        .setId(getLongValue(hist, "i"))
                        .setType(getIntValue(hist, "t"))
                        .setEffDate(getLongValue(hist, "e"))
                        .setExpDate(getLongValue(hist, "x"))
                        .setUpdateDate(getLongValue(hist, "u"))
                        .setState(getIntValue(hist, "s"))
                        .setLevel(getIntValue(hist, "l"))
                        .setOf(getLongValue(hist, "f"))
                        .setContent(getStringValue(hist, "n"))
                        .build();
                    builder.putHistory(Long.parseLong(entry.getKey().substring(1)), history);
                } catch (Exception e) {
                    logger.warn("[History Processing] Error processing history entry {}: {}", 
                        entry.getKey(), e.getMessage());
                }
            }
        }
    }

    public void sendBatch(KafkaProducer<byte[], byte[]> producer, 
                         List<ProducerRecord<byte[], byte[]>> records, 
                         int maxRetries) {
        if (records == null || records.isEmpty()) {
            return;
        }

        int retryCount = 0;
        boolean success = false;
        List<ProducerRecord<byte[], byte[]>> failedRecords = new ArrayList<>();

        while (!success && retryCount < maxRetries) {
            try {
                for (ProducerRecord<byte[], byte[]> record : records) {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            totalMessagesFailed.incrementAndGet();
                        } else {
                            incrementMessageCounters();
                        }
                    });
                }
                success = true;
            } catch (Exception e) {
                retryCount++;
                if (retryCount < maxRetries) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    failedRecords.addAll(records);
                }
            }
        }

        if (!failedRecords.isEmpty()) {
            totalMessagesFailed.addAndGet(failedRecords.size());
        }
    }

    public void logSkippedMessage(String key, String reason) {
        totalMessagesSkipped.incrementAndGet();
        logger.warn("[Message Skipped] Key: {}, Reason: {}", key, reason);
    }

    public void logFailedMessage(ProducerRecord<byte[], byte[]> record, String reason, Exception e) {
        logger.error("[Message Failed] Topic: {}, Key: {}, Reason: {}, Error: {}", 
            record.topic(), new String(record.key()), reason, e.getMessage());
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

