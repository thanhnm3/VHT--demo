package com.example.pipeline.service;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class AerospikeProducerService {
    private final ExecutorService executor;
    private final MessageProducerService messageService;
    private final String sourceNamespace;

    public AerospikeProducerService(ExecutorService executor, 
                                  MessageProducerService messageService,
                                  Map<String, String> prefixToTopicMap,
                                  String defaultTopic,
                                  String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
        
        // Initialize message service with topic mapping
        messageService.initializeTopicMapping(prefixToTopicMap, defaultTopic);
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
            System.out.println("Starting to read data from Aerospike namespace: " + sourceNamespace);
            client.scanAll(scanPolicy, sourceNamespace, setName, (key, record) -> {
                rateLimiter.acquire();

                executor.submit(() -> {
                    try {
                        if (!messageService.isValidRecord(record)) {
                            String keyStr = key.userKey != null ? key.userKey.toString() : "null";
                            messageService.logSkippedMessage(keyStr, "Invalid record structure");
                            return;
                        }

                        ProducerRecord<byte[], byte[]> kafkaRecord = messageService.createKafkaRecord(key, record);

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

            System.out.println("Finished scanning data from Aerospike namespace: " + sourceNamespace);
        } catch (Exception e) {
            System.err.println("Error scanning data from Aerospike: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 