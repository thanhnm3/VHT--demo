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
    private final String region;
    private volatile boolean isRunning = true;
    private final AtomicLong lastProcessedOffset = new AtomicLong(0);
    private final AtomicLong currentOffset = new AtomicLong(0);
    private final AtomicLong processedRecords = new AtomicLong(0);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MessageService(AerospikeClient destinationClient, WritePolicy writePolicy, 
                         String namespace, String setName, String region,
                         int workerPoolSize) {
        this.destinationClient = destinationClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.region = region;
        
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
                    thread.setName(region + "-worker-" + threadCount.getAndIncrement());
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
                    logger.error("[{}] Error processing record: {}", region, e.getMessage());
                }
            });
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            // Check if key is null
            if (record.key() == null) {
                logger.error("[{}] Error: Record key is null", region);
                return;
            }

            // Create Aerospike key using record key as PK
            Key key = new Key(namespace, setName, record.key());
            
            // Try to parse message if value is not null
            if (record.value() != null) {
                try {
                    String message = new String(record.value(), StandardCharsets.UTF_8);
                    Map<String, Object> data = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
                    
                    // Create bins for each field
                    List<Bin> bins = new ArrayList<>();
                    
                    // Add user_id if present
                    if (data.containsKey("user_id")) {
                        bins.add(new Bin("user_id", (String) data.get("user_id")));
                    }
                    
                    // Add phone if present
                    if (data.containsKey("phone")) {
                        bins.add(new Bin("phone", (String) data.get("phone")));
                    }
                    
                    // Add service_type if present
                    if (data.containsKey("service_type")) {
                        bins.add(new Bin("service_type", (String) data.get("service_type")));
                    }
                    
                    // Add province if present
                    if (data.containsKey("province")) {
                        bins.add(new Bin("province", (String) data.get("province")));
                    }
                    
                    // Add region if present
                    if (data.containsKey("region")) {
                        bins.add(new Bin("region", (String) data.get("region")));
                    }
                    
                    // Add last_updated if present
                    if (data.containsKey("last_updated")) {
                        Object lastUpdated = data.get("last_updated");
                        if (lastUpdated instanceof Number) {
                            bins.add(new Bin("last_updated", ((Number) lastUpdated).longValue()));
                        }
                    }
                    
                    // Add notes if present
                    if (data.containsKey("notes")) {
                        Object notes = data.get("notes");
                        if (notes instanceof String) {
                            bins.add(new Bin("notes", (String) notes));
                        } else if (notes instanceof byte[]) {
                            bins.add(new Bin("notes", (byte[]) notes));
                        }
                    }
                    
                    // Write all bins to Aerospike
                    if (!bins.isEmpty()) {
                        destinationClient.put(writePolicy, key, bins.toArray(new Bin[0]));
                    } else {
                        logger.warn("[{}] No valid data found in record", region);
                    }
                    
                } catch (Exception e) {
                    logger.error("[{}] Error parsing message: {}", region, e.getMessage());
                }
            } else {
                logger.warn("[{}] Record value is null", region);
            }
            
        } catch (Exception e) {
            logger.error("[{}] Error writing to Aerospike: {}", region, e.getMessage(), e);
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
                    logger.error("[{}] Error processing record: {}", region, e.getMessage());
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
