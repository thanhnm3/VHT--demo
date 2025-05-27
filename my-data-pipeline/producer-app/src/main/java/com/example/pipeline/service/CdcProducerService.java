package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.RecordSet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CdcProducerService {
    private final ExecutorService executor;
    private final MessageProducerService messageService;
    private final Map<String, String> prefixToTopicMap;
    private final String defaultTopic;
    private final String sourceNamespace;
    private long lastPolledTime;
    private final AtomicInteger messagesSentThisSecond;
    private final ScheduledExecutorService scheduler;

    public CdcProducerService(ExecutorService executor, MessageProducerService messageService,
                            Map<String, String> prefixToTopicMap, String defaultTopic,
                            String sourceNamespace) {
        this.executor = executor;
        this.messageService = messageService;
        this.prefixToTopicMap = prefixToTopicMap;
        this.defaultTopic = defaultTopic;
        this.sourceNamespace = sourceNamespace;
        this.lastPolledTime = System.currentTimeMillis() - 10_000; // Bắt đầu từ 10s trước
        this.messagesSentThisSecond = new AtomicInteger(0);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Khởi tạo message service với topic mapping
        messageService.initializeTopicMapping(prefixToTopicMap, defaultTopic);
        
        // Khởi tạo scheduler để theo dõi số lượng message
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("[CDC Producer] Messages sent this second: " + messagesSentThisSecond.get());
            messagesSentThisSecond.set(0);
        }, 0, 1, TimeUnit.SECONDS);
    }

    public void readDataFromAerospike(AerospikeClient aerospikeClient, KafkaProducer<byte[], byte[]> producer,
                                    double currentRate, String setName, int maxRetries) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                long windowStart = lastPolledTime;
                long windowEnd = System.currentTimeMillis();
                
                System.out.printf("[CDC Producer] Scanning window [%d ? %d]%n", windowStart, windowEnd);
                
                Statement stmt = new Statement();
                stmt.setNamespace(sourceNamespace);
                stmt.setSetName(setName);
                
                RecordSet records = aerospikeClient.query(null, stmt);
                try {
                    while (records.next()) {
                        Key key = records.getKey();
                        Record record = records.getRecord();
                        
                        if (key != null && key.userKey != null) {
                            long updateTime = record != null ? record.getLong("lastUpdate") : System.currentTimeMillis();
                            
                            if (updateTime > windowStart) {
                                ProducerRecord<byte[], byte[]> kafkaRecord = messageService.createKafkaRecord(key, record);
                                if (kafkaRecord != null) {
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
                System.out.printf("[CDC Producer] Window done. Next start = %d%n", lastPolledTime);
                
                // Đợi một khoảng thời gian trước khi quét tiếp
                Thread.sleep((long) (1000 / currentRate));
                
            } catch (Exception e) {
                System.err.println("[CDC Producer] Error during scan, retrying same window: " + e.getMessage());
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