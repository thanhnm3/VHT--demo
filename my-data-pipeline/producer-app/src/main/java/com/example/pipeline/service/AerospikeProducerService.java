package com.example.pipeline.service;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import com.google.common.util.concurrent.RateLimiter;
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


    public AerospikeProducerService(ExecutorService executor, 
                                  MessageProducerService messageService,
                                  String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
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
                        if (record == null) {
                            return;
                        }

                        // Lấy region từ record
                        String recordRegion = record.getString("region");
                        if (recordRegion == null) {
                            return;
                        }

                        // Kiểm tra consumers cho region này
                        List<String> consumers = messageService.getConsumersForRegion(recordRegion);
                        if (consumers == null || consumers.isEmpty()) {
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
                        }

                        if (messageService.hasPendingProducerMessages()) {
                            messageService.processPendingProducerMessages(producer, maxRetries);
                        }

                    } catch (Exception e) {
                        logger.error("Error processing record: {}", e.getMessage());
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
        } catch (Exception e) {
            logger.error("Error scanning data from Aerospike: {}", e.getMessage());
        }
    }

} 