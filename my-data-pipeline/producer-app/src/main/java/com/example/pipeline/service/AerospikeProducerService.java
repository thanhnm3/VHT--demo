package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ScanPolicy;
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
    private final Map<String, String> regionToTopicMap;
    private volatile double currentRate;
    private volatile ScanPolicy scanPolicy;
    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private final AtomicLong startTime = new AtomicLong(0);
    private final AtomicLong totalScannedRecords = new AtomicLong(0);
    private final AtomicLong totalSkippedRecords = new AtomicLong(0);
    private final AtomicLong totalFailedRecords = new AtomicLong(0);
    private ScheduledExecutorService monitorExecutor;
    private static final int MONITORING_INTERVAL_MS = 1000; // 1 second

    public AerospikeProducerService(ExecutorService executor, 
                                  MessageProducerService messageService,
                                  Map<String, String> regionToTopicMap,
                                  String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
        this.regionToTopicMap = regionToTopicMap;
        this.currentRate = 5000.0; // Default rate
        
        // Initialize message service with topic mapping
        messageService.initializeTopicMapping(regionToTopicMap);
    }

    private String extractRegion(Record record) {
        if (record == null || record.bins == null) {
            return null;
        }
        return (String) record.getValue("region");
    }

    private void startRateMonitoring() {
        if (monitorExecutor != null) {
            monitorExecutor.shutdown();
        }
        
        monitorExecutor = Executors.newSingleThreadScheduledExecutor();
        startTime.set(System.currentTimeMillis());
        
        monitorExecutor.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - startTime.get();
            if (elapsedTime > 0) {
                double actualRate = (totalRecordsProcessed.get() * 1000.0) / elapsedTime;
                logger.info("Rate monitoring - Actual: {}/second, Target: {}/second", 
                    String.format("%.2f", actualRate),
                    String.format("%.2f", scanPolicy.recordsPerSecond));
                logger.info("Records status - Scanned: {}, Processed: {}, Skipped: {}, Failed: {}", 
                    totalScannedRecords.get(),
                    totalRecordsProcessed.get(),
                    totalSkippedRecords.get(),
                    totalFailedRecords.get());
            }
        }, 0, MONITORING_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void updateRate(double newRate) {
        double oldRate = this.currentRate;
        this.currentRate = newRate;
        if (scanPolicy != null) {
            scanPolicy.recordsPerSecond = (int) newRate;
            // Chỉ log khi rate thay đổi đáng kể (> 10%)
            if (Math.abs(newRate - oldRate) / oldRate > 0.1) {
                logger.info("Updated scan rate to: {}/second", newRate);
            }
        }
    }

    public void readDataFromAerospike(AerospikeClient client, 
                                    KafkaProducer<byte[], byte[]> producer,
                                    double initialRate,
                                    String setName,
                                    int maxRetries) {
        scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = false;
        scanPolicy.maxConcurrentNodes = 1;
        scanPolicy.recordsPerSecond = (int) initialRate;
        scanPolicy.socketTimeout = 5000;
        scanPolicy.maxRetries = 5;
        scanPolicy.sleepBetweenRetries = 1000;

        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        Object batchLock = new Object();
        final AtomicLong lastBatchTime = new AtomicLong(System.currentTimeMillis());
        final long BATCH_INTERVAL_MS = 1000;

        try {
            logger.info("Starting to read data from Aerospike namespace: {} with rate limit: {}/second", 
                sourceNamespace, initialRate);
            
            startRateMonitoring();
            
            client.scanAll(scanPolicy, sourceNamespace, setName, (key, record) -> {
                totalScannedRecords.incrementAndGet();
                executor.submit(() -> {
                    try {
                        if (!messageService.isValidRecord(record)) {
                            totalSkippedRecords.incrementAndGet();
                            String keyStr = key.userKey != null ? key.userKey.toString() : "null";
                            messageService.logSkippedMessage(keyStr, "Invalid record structure");
                            return;
                        }

                        // Lấy key dạng byte array
                        byte[] keyBytes = (byte[]) key.userKey.getObject();
                        if (keyBytes == null) {
                            totalSkippedRecords.incrementAndGet();
                            messageService.logSkippedMessage("null", "Key is null");
                            return;
                        }

                        // Lấy region từ record
                        String region = extractRegion(record);
                        if (region == null) {
                            totalSkippedRecords.incrementAndGet();
                            messageService.logSkippedMessage(new String(keyBytes), "Region field is null or invalid");
                            return;
                        }

                        // Kiểm tra xem region có trong map không
                        if (!regionToTopicMap.containsKey(region)) {
                            totalSkippedRecords.incrementAndGet();
                            messageService.logSkippedMessage(new String(keyBytes), 
                                "No topic mapping found for region: " + region);
                            return;
                        }

                        ProducerRecord<byte[], byte[]> kafkaRecord = messageService.createKafkaRecord(key, record);
                        if (kafkaRecord != null) {
                            synchronized (batchLock) {
                                batch.add(kafkaRecord);
                                totalRecordsProcessed.incrementAndGet();

                                long currentTime = System.currentTimeMillis();
                                if (batch.size() >= 100 || 
                                    (batch.size() > 0 && currentTime - lastBatchTime.get() >= BATCH_INTERVAL_MS)) {
                                    messageService.sendBatch(producer, new ArrayList<>(batch), maxRetries);
                                    batch.clear();
                                    lastBatchTime.set(currentTime);
                                }
                            }
                        } else {
                            totalFailedRecords.incrementAndGet();
                        }

                        if (messageService.hasPendingProducerMessages()) {
                            messageService.processPendingProducerMessages(producer, maxRetries);
                        }

                    } catch (Exception e) {
                        totalFailedRecords.incrementAndGet();
                        messageService.logFailedMessage(messageService.createKafkaRecord(key, record), 
                                                      "Processing error", e);
                    }
                });
            });

            // Process any remaining messages
            while (messageService.hasPendingProducerMessages()) {
                messageService.processPendingProducerMessages(producer, maxRetries);
            }

            synchronized (batchLock) {
                if (!batch.isEmpty()) {
                    messageService.sendBatch(producer, new ArrayList<>(batch), maxRetries);
                    batch.clear();
                }
            }

            // Đảm bảo tất cả message được gửi đi
            try {
                long messagesInBatch = batch.size();
                long pendingMessages = messageService.getPendingMessageCount();
                producer.flush();
                logger.info("Flushed messages to Kafka - Batch size: {}, Pending messages: {}, Total: {}", 
                    messagesInBatch, pendingMessages, messagesInBatch + pendingMessages);
            } catch (Exception e) {
                logger.error("Error flushing Kafka producer: {}", e.getMessage());
            }

            // Đóng executor và chờ tất cả task hoàn thành
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                    logger.warn("Executor did not terminate in time. Forcing shutdown...");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            logger.info("Finished scanning data from Aerospike namespace: {}. Summary:", sourceNamespace);
            logger.info("- Total records scanned: {}", totalScannedRecords.get());
            logger.info("- Total records processed: {}", totalRecordsProcessed.get());
            logger.info("- Total records skipped: {}", totalSkippedRecords.get());
            logger.info("- Total records failed: {}", totalFailedRecords.get());
            logger.info("- Success rate: {}%", 
                String.format("%.2f", (totalRecordsProcessed.get() * 100.0) / totalScannedRecords.get()));

        } catch (Exception e) {
            logger.error("Error scanning data from Aerospike: {}", e.getMessage());
            e.printStackTrace();
        } finally {
            if (monitorExecutor != null) {
                monitorExecutor.shutdown();
                try {
                    if (!monitorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        monitorExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    monitorExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
} 