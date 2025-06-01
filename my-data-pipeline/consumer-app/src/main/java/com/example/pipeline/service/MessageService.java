package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;

public class MessageService {
    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);
    private final AerospikeClient destinationClient;
    private final WritePolicy writePolicy;
    private final ExecutorService workerPool;
    private final String namespace;
    private final String setName;
    private final String prefix;
    private volatile boolean isRunning = true;
    private final AtomicLong lastProcessedOffset = new AtomicLong(0);
    private final AtomicLong currentOffset = new AtomicLong(0);
    private final AtomicLong processedRecords = new AtomicLong(0);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MessageService(AerospikeClient destinationClient, WritePolicy writePolicy, 
                         String namespace, String setName, String prefix,
                         int workerPoolSize) {
        this.destinationClient = destinationClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.prefix = prefix;
        
        // Create worker pool with CallerRunsPolicy
        this.workerPool = new ThreadPoolExecutor(
            workerPoolSize,
            workerPoolSize,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactory() {
                private final AtomicInteger threadCount = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName(prefix + "-worker-" + threadCount.getAndIncrement());
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void processRecords(ConsumerRecords<byte[], byte[]> records) {
        if (!isRunning || records.isEmpty()) return;

        List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            recordList.add(record);
            currentOffset.set(record.offset());
        }

        processBatch(recordList);
    }

    private void processBatch(List<ConsumerRecord<byte[], byte[]>> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (!isRunning) break;

            workerPool.submit(() -> {
                try {
                    processRecord(record);
                    // Update last processed offset after successful processing
                    lastProcessedOffset.set(record.offset());
                    processedRecords.incrementAndGet();
                } catch (Exception e) {
                    logger.error("[{}] Error processing record: {}", prefix, e.getMessage());
                }
            });
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            // Check if key is null
            if (record.key() == null) {
                logger.error("[{}] Error: Record key is null", prefix);
                return;
            }

            // Create Aerospike key using record key as PK
            Key key = new Key(namespace, setName, record.key());
            
            // Initialize default values
            byte[] personData = null;
            long lastUpdate = System.currentTimeMillis();
            
            // Try to parse message if value is not null
            if (record.value() != null) {
                try {
                    String message = new String(record.value(), StandardCharsets.UTF_8);
                    Map<String, Object> data = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
                    
                    // Parse personData
                    String personDataBase64 = (String) data.get("personData");
                    if (personDataBase64 != null) {
                        try {
                            personData = Base64.getDecoder().decode(personDataBase64);
                        } catch (IllegalArgumentException e) {
                            logger.error("[{}] Error decoding personData: {}", prefix, e.getMessage());
                        }
                    }
                    
                    // Parse lastUpdate
                    Object lastUpdateObj = data.get("lastUpdate");
                    if (lastUpdateObj != null) {
                        try {
                            lastUpdate = ((Number) lastUpdateObj).longValue();
                        } catch (Exception e) {
                            logger.error("[{}] Error parsing lastUpdate: {}", prefix, e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    logger.error("[{}] Error parsing message: {}", prefix, e.getMessage());
                }
            }
            
            // Create bins with available data
            Bin personDataBin = new Bin("personData", personData);
            Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
            
            // Write to Aerospike with both bins
            destinationClient.put(writePolicy, key, personDataBin, lastUpdateBin);
            
        } catch (Exception e) {
            logger.error("[{}] Error writing to Aerospike: {}", prefix, e.getMessage(), e);
            throw e;
        }
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public long getLastProcessedOffset() {
        return lastProcessedOffset.get();
    }

    public void shutdown() {
        isRunning = false;
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
        }
    }

    public void processRecordsAndWait(ConsumerRecords<byte[], byte[]> records) {
        if (!isRunning || records.isEmpty()) return;
        List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            recordList.add(record);
            currentOffset.set(record.offset());
        }
        CountDownLatch latch = new CountDownLatch(recordList.size());
        for (ConsumerRecord<byte[], byte[]> record : recordList) {
            if (!isRunning) {
                latch.countDown();
                continue;
            }
            workerPool.submit(() -> {
                try {
                    processRecord(record);
                    lastProcessedOffset.set(record.offset());
                    processedRecords.incrementAndGet();
                } catch (Exception e) {
                    logger.error("[{}] Error processing record: {}", prefix, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
} 