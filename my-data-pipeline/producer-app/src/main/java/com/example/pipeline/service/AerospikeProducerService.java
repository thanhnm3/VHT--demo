package com.example.pipeline.service;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AerospikeProducerService {
    private static final Logger logger = LoggerFactory.getLogger(AerospikeProducerService.class);
    private final ExecutorService executor;
    private final MessageProducerService messageService;
    private final String sourceNamespace;
    private final Map<String, String> prefixToTopicMap;

    public AerospikeProducerService(ExecutorService executor, 
                                  MessageProducerService messageService,
                                  Map<String, String> prefixToTopicMap,
                                  String defaultTopic,
                                  String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
        this.prefixToTopicMap = prefixToTopicMap;
        
        // Initialize message service with topic mapping
        messageService.initializeTopicMapping(prefixToTopicMap, defaultTopic);
    }

    private String extractPrefix(byte[] key) {
        if (key == null || key.length < 3) {
            return null;
        }
        return new String(key, 0, 3, StandardCharsets.UTF_8);
    }

    public void readDataFromAerospike(AerospikeClient client, 
                                    KafkaProducer<byte[], byte[]> producer,
                                    double currentRate,
                                    String setName,
                                    int maxRetries) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.maxConcurrentNodes = 4;
        scanPolicy.recordsPerSecond = (int) currentRate;

        RateLimiter rateLimiter = RateLimiter.create(currentRate);
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        Object batchLock = new Object();
        final AtomicLong lastBatchTime = new AtomicLong(System.currentTimeMillis());
        final long BATCH_INTERVAL_MS = 1000;

        try {
            logger.info("Starting to read data from Aerospike namespace: {}", sourceNamespace);
            client.scanAll(scanPolicy, sourceNamespace, setName, (key, record) -> {
                rateLimiter.acquire();

                executor.submit(() -> {
                    try {
                        if (!messageService.isValidRecord(record)) {
                            String keyStr = key.userKey != null ? key.userKey.toString() : "null";
                            messageService.logSkippedMessage(keyStr, "Invalid record structure");
                            return;
                        }

                        // Lấy key dạng byte array
                        byte[] keyBytes = (byte[]) key.userKey.getObject();
                        if (keyBytes == null) {
                            messageService.logSkippedMessage("null", "Key is null");
                            return;
                        }

                        // Lấy prefix từ key
                        String prefix = extractPrefix(keyBytes);
                        if (prefix == null) {
                            messageService.logSkippedMessage(new String(keyBytes), "Invalid key format");
                            return;
                        }

                        // Kiểm tra xem prefix có trong map không
                        if (!prefixToTopicMap.containsKey(prefix)) {
                            messageService.logSkippedMessage(new String(keyBytes), 
                                "No topic mapping found for prefix: " + prefix);
                            return;
                        }

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
                        }

                        if (messageService.hasPendingProducerMessages()) {
                            messageService.processPendingProducerMessages(producer, maxRetries);
                        }

                    } catch (Exception e) {
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

            logger.info("Finished scanning data from Aerospike namespace: {}", sourceNamespace);
        } catch (Exception e) {
            logger.error("Error scanning data from Aerospike: {}", e.getMessage());
            e.printStackTrace();
        }
    }
} 