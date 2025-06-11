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

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.charset.StandardCharsets;

public class CdcProducerService {
    private final ExecutorService executor;
    private final MessageProducerService messageService;
    private final String sourceNamespace;
    private long lastPolledTime;
    private final AtomicInteger messagesSentThisSecond;
    private final ScheduledExecutorService scheduler;
    private static final Logger logger = LoggerFactory.getLogger(CdcProducerService.class);

    public CdcProducerService(ExecutorService executor,
                            MessageProducerService messageService,
                            String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
        this.lastPolledTime = System.currentTimeMillis() - 10_000;
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
                            long updateTime = record != null && record.getValue("lastUpdate") != null ? 
                                (long) record.getValue("lastUpdate") : 
                                System.currentTimeMillis();
                            
                            if (updateTime > windowStart) {
                                // Lấy key dạng byte array
                                byte[] keyBytes = (byte[]) key.userKey.getObject();
                                if (keyBytes == null) {
                                    logger.warn("Key is null");
                                    continue;
                                }

                                // Lấy province từ key
                                String province = extractProvince(keyBytes);
                                if (province == null) {
                                    logger.warn("Invalid key format: {}", new String(keyBytes));
                                    continue;
                                }

                                // Lấy region từ province
                                String region = messageService.getRegionOfProvince(province);
                                if (region == null) {
                                    logger.warn("No region found for province: {}", province);
                                    continue;
                                }

                                // Lấy danh sách consumers cho region
                                List<String> consumers = messageService.getConsumersForRegion(region);
                                if (consumers == null || consumers.isEmpty()) {
                                    logger.warn("No consumers found for region: {}", region);
                                    continue;
                                }

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
                                                        logger.info("[{}] Sent {} messages to region {}", 
                                                                 province, messagesSentThisSecond.get(), region);
                                                    }
                                                }
                                            });
                                        } catch (Exception e) {
                                            messageService.logFailedMessage(kafkaRecord, "Error sending message", e);
                                        }
                                    });
                                }
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

    private String extractProvince(byte[] key) {
        if (key == null || key.length < 3) {
            return null;
        }
        return new String(key, 0, 3, StandardCharsets.UTF_8);
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