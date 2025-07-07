package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.policy.WritePolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;

public class CdcProducerService {
    private final ExecutorService executor;
    private final MessageProducerService messageService;
    private final String sourceNamespace;
    private long lastPolledTime;
    private final AtomicInteger messagesSentThisSecond;
    private final AtomicLong totalRecords;
    private final ScheduledExecutorService scheduler;
    private static final Logger logger = LoggerFactory.getLogger(CdcProducerService.class);
    
    // CDC Configuration
    private static final int CDC_WINDOW_SIZE_MS = 2000; // 2 seconds window
    private static final String LU_INDEX_NAME = "lu_index";
    private boolean indexCreated = false;

    public CdcProducerService(ExecutorService executor,
                            MessageProducerService messageService,
                            String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.sourceNamespace = sourceNamespace;
        this.lastPolledTime = System.currentTimeMillis() - CDC_WINDOW_SIZE_MS;
        this.messagesSentThisSecond = new AtomicInteger(0);
        this.totalRecords = new AtomicLong(0);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Tạo secondary index trên trường lu (last update)
     */
    private boolean createLuIndex(AerospikeClient client, String setName) {
        if (indexCreated) return true;
        
        try {
            // Kiểm tra xem index đã tồn tại chưa
            try {
                // Thử tạo index
                client.createIndex(null, sourceNamespace, setName, LU_INDEX_NAME, "lu", IndexType.NUMERIC);
                logger.info("[CDC Producer] Created secondary index '{}' on field 'lu' for namespace: {}, set: {}", 
                           LU_INDEX_NAME, sourceNamespace, setName);
                indexCreated = true;
                
                // Đợi 30 giây để index được build xong hoàn toàn
                logger.info("[CDC Producer] Waiting 30 seconds for index to be fully built...");
                for (int i = 30; i > 0; i--) {
                    logger.info("[CDC Producer] Index building in progress... {} seconds remaining", i);
                    Thread.sleep(1000);
                }
                logger.info("[CDC Producer] Index build wait completed. Proceeding with CDC operations.");
                return true;
                
            } catch (Exception e) {
                if (e.getMessage().contains("already exists")) {
                    logger.info("[CDC Producer] Index '{}' already exists - proceeding with CDC", LU_INDEX_NAME);
                    indexCreated = true;
                    return true;
                } else {
                    throw e; // Re-throw nếu là lỗi khác
                }
            }
            
        } catch (Exception e) {
            logger.error("[CDC Producer] Failed to create/verify index '{}': {}", LU_INDEX_NAME, e.getMessage());
            logger.error("[CDC Producer] CDC cannot proceed without index. Stopping CDC producer.");
            return false;
        }
    }

    public void readDataFromAerospike(AerospikeClient client,
                                    KafkaProducer<byte[], byte[]> producer,
                                    double currentRate,
                                    String setName,
                                    int maxRetries) {
        logger.info("Starting CDC data reading from Aerospike namespace: {}", sourceNamespace);
        logger.info("CDC scan policy: currentRate={} messages/second, window size: {}ms", currentRate, CDC_WINDOW_SIZE_MS);
        
        // Tạo/kiểm tra index trước khi bắt đầu query
        if (!createLuIndex(client, setName)) {
            logger.error("[CDC Producer] Cannot start CDC without index. Exiting.");
            return; // Dừng luôn nếu không tạo được index
        }
        
        logger.info("[CDC Producer] Index verified successfully. Starting CDC scanning...");
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                long windowStart = lastPolledTime;
                long windowEnd = System.currentTimeMillis();
                
                logger.info("[CDC Producer] Scanning window [{} ==> {}] ({}ms)", 
                           windowStart, windowEnd, windowEnd - windowStart);
                
                // Query với filter theo thời gian trên trường lu
                Statement stmt = new Statement();
                stmt.setNamespace(sourceNamespace);
                stmt.setSetName(setName);
                
                // Tạo filter cho cửa sổ thời gian 2s
                Filter filter = Filter.range("lu", windowStart, windowEnd);
                stmt.setFilter(filter);
                
                RecordSet records = client.query(null, stmt);
                int recordsInWindow = 0;
                
                try {
                    while (records.next()) {
                        Key key = records.getKey();
                        Record record = records.getRecord();
                        
                        if (key != null && key.userKey != null) {
                            Object subObj = record.getValue("sub");
                            Map<String, Object> sub = null;
                            if (subObj instanceof Map) {
                                sub = (Map<String, Object>) subObj;
                            }
                            if (sub != null) {
                                // Lấy thời gian last update từ trường lu
                                long updateTime = System.currentTimeMillis();
                                Object luObj = record.getValue("lu");
                                if (luObj instanceof Number) {
                                    updateTime = ((Number) luObj).longValue();
                                }
                                
                                Object ctrlObj = record.getValue("ctrl");
                                if (ctrlObj instanceof byte[]) {
                                    byte[] ctrlData = (byte[]) ctrlObj;
                                    logger.debug("[CDC] Control data size: {} bytes for key: {}", ctrlData.length, key.userKey);
                                }
                                
                                // Kiểm tra lại thời gian (để đảm bảo chính xác)
                                if (updateTime >= windowStart && updateTime < windowEnd) {
                                    String region = (String) sub.get("r");
                                    if (region != null) {
                                        ProducerRecord<byte[], byte[]> kafkaRecord = messageService.createKafkaRecord(key, record);
                                        if (kafkaRecord != null) {
                                            totalRecords.incrementAndGet();
                                            recordsInWindow++;
                                            
                                            executor.submit(() -> {
                                                try {
                                                    producer.send(kafkaRecord, (metadata, exception) -> {
                                                        if (exception != null) {
                                                            messageService.logFailedMessage(kafkaRecord, "Failed to send message", exception);
                                                        } else {
                                                            messagesSentThisSecond.incrementAndGet();
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
                
                // Log kết quả của window
                logger.info("[CDC Producer] Window completed: {} records found in {}ms window", 
                           recordsInWindow, windowEnd - windowStart);
                
                // Cập nhật lastPolledTime cho window tiếp theo
                lastPolledTime = windowEnd;
                logger.debug("[CDC Producer] Next window start = {}", lastPolledTime);
                
                // Đợi để đảm bảo window 2s
                long sleepTime = Math.max(0, CDC_WINDOW_SIZE_MS - (System.currentTimeMillis() - windowEnd));
                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
                
            } catch (Exception e) {
                logger.error("[CDC Producer] Error during scan, retrying same window: {}", e.getMessage());
                e.printStackTrace();
            }
        }
        
        // Log final statistics
        logger.info("Finished CDC data reading from Aerospike namespace: {}", sourceNamespace);
        logger.info("CDC Statistics - Total records processed: {}", totalRecords.get());
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