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
                            long updateTime = record != null && record.getValue("last_updated") != null ? 
                                (long) record.getValue("last_updated") : 
                                System.currentTimeMillis();
                            
                            if (updateTime > windowStart) {
                                // Kiểm tra xem record có phải là delete không
                                boolean isDeleted = isRecordDeleted(record);
                                
                                // Nếu là delete hoặc có region, xử lý record
                                if (isDeleted || (record != null && record.getValue("region") != null)) {
                                    // Sử dụng MessageProducerService để xử lý message
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
                                                            String region = isDeleted ? "DELETED" : (String) record.getValue("region");
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
                                    messageService.logSkippedMessage(key.userKey.toString(), "Invalid record state");
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

    private boolean isRecordDeleted(Record record) {
        if (record == null) return false;
        
        // Kiểm tra nếu tất cả các trường đều null trừ last_updated và region
        return record.getValue("user_id") == null &&
               record.getValue("phone") == null &&
               record.getValue("service_type") == null &&
               record.getValue("province") == null &&
               record.getValue("notes") == null &&
               record.getValue("last_updated") != null;
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