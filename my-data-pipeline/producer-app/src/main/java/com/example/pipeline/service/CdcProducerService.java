package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.RecordSet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;

public class CdcProducerService {
    private final ExecutorService executor;
    private final AllProducerService messageService;
    private final String sourceNamespace;
    private long lastPolledTime;
    private final AtomicInteger messagesSentThisSecond;
    private final ScheduledExecutorService scheduler;
    private static final Logger logger = LoggerFactory.getLogger(CdcProducerService.class);

    public CdcProducerService(ExecutorService executor,
                            AllProducerService messageService,
                            String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
        this.lastPolledTime = System.currentTimeMillis() - 10;
        this.messagesSentThisSecond = new AtomicInteger(0);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    public void readDataFromAerospike(AerospikeClient client,
                                    KafkaProducer<byte[], byte[]> producer,
                                    double currentRate,
                                    String setName,
                                    int maxRetries) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                long windowStart = lastPolledTime;
                long windowEnd = System.currentTimeMillis();
                
                logger.info("[CDC Producer] Scanning window [{} ==> {}]", windowStart, windowEnd);
                
                Statement stmt = new Statement();
                stmt.setNamespace(sourceNamespace);
                stmt.setSetName(setName);
                
                RecordSet records = client.query(null, stmt);
                try {
                    while (records.next()) {
                        Key key = records.getKey();
                        Record record = records.getRecord();
                        
                        if (key != null && key.userKey != null) {
                            long updateTime = System.currentTimeMillis();
                            if (record != null && record.getValue("sub") instanceof Map) {
                                Map<String, Object> sub = (Map<String, Object>) record.getValue("sub");
                                if (sub.get("lu") instanceof Number) {
                                    updateTime = ((Number) sub.get("lu")).longValue();
                                }
                                String region = (String) sub.get("r");
                                if (updateTime > windowStart && region != null) {
                                    ProducerRecord<byte[], byte[]> kafkaRecord = messageService.createKafkaRecord(key, record);
                                    if (kafkaRecord != null) {
                                        executor.submit(() -> {
                                            try {
                                                producer.send(kafkaRecord, (metadata, exception) -> {
                                                    if (exception != null) {
                                                        messageService.logFailedMessage(kafkaRecord, "Failed to send message", exception);
                                                    } else {
                                                        messagesSentThisSecond.incrementAndGet();
                                                        if (messagesSentThisSecond.get() % 1000 == 0) {
                                                            logger.info("[CDC Producer] Sent {} messages for region {}", 
                                                                messagesSentThisSecond.get(), region);
                                                        }
                                                    }
                                                });
                                            } catch (Exception e) {
                                                messageService.logFailedMessage(kafkaRecord, "Error sending message", e);
                                            }
                                        });
                                    }
                                } else {
                                    messageService.logSkippedMessage(key.userKey.toString(), "Invalid or missing region in sub bin");
                                }
                            } else {
                                messageService.logSkippedMessage(key.userKey.toString(), "Missing or invalid sub bin");
                            }
                        } else {
                            messageService.logSkippedMessage("null", "Invalid key");
                        }
                    }
                } finally {
                    records.close();
                }
                
                // Nếu query thành công, cập nhật lastPolledTime
                lastPolledTime = windowEnd;
                logger.info("[CDC Producer] Window done. Next start = {}", lastPolledTime);
                
                // Đợi một khoảng thời gian trước khi quét tiếp
                Thread.sleep((long) (1000 / currentRate));
                
            } catch (Exception e) {
                logger.error("[CDC Producer] Error during scan, retrying same window: {}", e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.MINUTES)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
} 