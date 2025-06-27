package com.example.pipeline.service;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AerospikeProducerService {
    private static final Logger logger = LoggerFactory.getLogger(AerospikeProducerService.class);
    private final ExecutorService executor;
    private final MessageProducerService messageService;
    private final String sourceNamespace;
    private volatile com.google.common.util.concurrent.RateLimiter rateLimiter;

    public AerospikeProducerService(ExecutorService executor, 
                                  MessageProducerService messageService,
                                  String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
        this.rateLimiter = null;
    }

    public void readDataFromAerospike(AerospikeClient client, 
                                    KafkaProducer<byte[], byte[]> producer,
                                    double currentRate,
                                    String setName,
                                    int maxRetries) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = false;
        scanPolicy.maxConcurrentNodes = 1;
        scanPolicy.recordsPerSecond = (int) currentRate;

        if (this.rateLimiter == null) {
            this.rateLimiter = com.google.common.util.concurrent.RateLimiter.create(currentRate);
        } else {
            this.rateLimiter.setRate(currentRate);
        }
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        Object batchLock = new Object();
        final AtomicLong lastBatchTime = new AtomicLong(System.currentTimeMillis());
        final long BATCH_INTERVAL_MS = 1000;
        final AtomicLong totalRecords = new AtomicLong(0);


        try {
            logger.info("Starting to read data from Aerospike namespace: {}", sourceNamespace);
            logger.info("Scan policy: concurrentNodes={}, maxConcurrentNodes={}, recordsPerSecond={}", 
                scanPolicy.concurrentNodes, scanPolicy.maxConcurrentNodes, scanPolicy.recordsPerSecond);
            
            client.scanAll(scanPolicy, sourceNamespace, setName, (key, record) -> {
                rateLimiter.acquire();
                totalRecords.incrementAndGet();

                executor.submit(() -> {
                    try {
                        if (record == null) {
                            logger.warn("Skipped null record for key: {}", key);
                            return;
                        }

                        // Lấy subscriber data từ bin "sub"
                        Object subValue = record.getValue("sub");
                        if (!(subValue instanceof Map)) {
                            logger.warn("Skipped record - Invalid subscriber data format for key: {}", key.userKey);
                            return;
                        }

                        @SuppressWarnings("unchecked")
                        Map<String, Object> subscriberData = (Map<String, Object>) subValue;
                        
                        // Lấy region từ trường "r" trong subscriber data
                        String recordRegion = (String) subscriberData.get("r");
                        if (recordRegion == null) {
                            logger.warn("Skipped record - No region found in subscriber data for key: {}", key.userKey);
                            return;
                        }

                        // Đọc bin "ctrl" để kiểm soát kích cỡ dữ liệu (không bắt buộc)
                        Object ctrlObj = record.getValue("ctrl");
                        if (ctrlObj instanceof byte[]) {
                            byte[] ctrlData = (byte[]) ctrlObj;
                            if (logger.isDebugEnabled()) {
                                logger.debug("[AerospikeProducer] Control data size: {} bytes for key: {}", ctrlData.length, key.userKey);
                            }
                        }

                        // Kiểm tra consumers cho region này
                        List<String> consumers = messageService.getConsumersForRegion(recordRegion);
                        if (consumers == null || consumers.isEmpty()) {
                            logger.warn("Skipped record - No consumers found for region: {}, key: {}", 
                                recordRegion, key.userKey);
                            return;
                        }

                        // Tạo và gửi Kafka record
                        ProducerRecord<byte[], byte[]> kafkaRecord = messageService.createKafkaRecord(key, record);
                        if (kafkaRecord != null) {
                            synchronized (batchLock) {
                                batch.add(kafkaRecord);

                                long currentTime = System.currentTimeMillis();
                                if (batch.size() >= 100 || 
                                    (batch.size() > 0 && currentTime - lastBatchTime.get() >= BATCH_INTERVAL_MS)) {
                                    messageService.sendBatch(producer, new ArrayList<>(batch), maxRetries);
                                    batch.clear();
                                    lastBatchTime.set(currentTime);
                                }
                            }
                        } else {
                            logger.warn("Failed to create Kafka record for key: {}", key.userKey);
                        }

                        if (messageService.hasPendingProducerMessages()) {
                            messageService.processPendingProducerMessages(producer, maxRetries);
                        }

                    } catch (Exception e) {
                        logger.error("Error processing record for key {}: {}", key.userKey, e.getMessage(), e);
                        messageService.logFailedMessage(messageService.createKafkaRecord(key, record), 
                                                      "Processing error", e);
                    }
                });
            });

            while (messageService.hasPendingProducerMessages()) {
                messageService.processPendingProducerMessages(producer, maxRetries);
            }

            synchronized (batchLock) {
                if (!batch.isEmpty()) {
                    messageService.sendBatch(producer, new ArrayList<>(batch), maxRetries);
                    batch.clear();
                }
            }

            // Ensure all messages are sent before finishing
            producer.flush();
            logger.info("Finished scanning data from Aerospike namespace: {}", sourceNamespace);
            logger.info("Statistics - Total records: {}", 
                totalRecords.get());
        } catch (Exception e) {
            logger.error("Error scanning data from Aerospike: {}", e.getMessage(), e);
        }
    }

    public void updateRate(double newRate) {
        if (this.rateLimiter != null) {
            this.rateLimiter.setRate(newRate);
            logger.info("[AerospikeProducerService] Updated RateLimiter to {} msg/sec", newRate);
        }
    }

} 