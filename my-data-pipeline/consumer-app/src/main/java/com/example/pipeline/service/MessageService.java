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
import com.fasterxml.jackson.core.JsonProcessingException;
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
            
            // Try to parse message if value is not null
            if (record.value() != null) {
                try {
                    String message = new String(record.value(), StandardCharsets.UTF_8);
                    Map<String, Object> data = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
                    
                    // Create bins list for Aerospike
                    List<Bin> bins = new ArrayList<>();
                    
                    // Add basic fields with original data types
                    if (data.containsKey("user_id")) {
                        Object userId = data.get("user_id");
                        bins.add(new Bin("user_id", userId != null ? userId.toString() : null));
                    }
                    if (data.containsKey("phone")) {
                        Object phone = data.get("phone");
                        bins.add(new Bin("phone", phone != null ? phone.toString() : null));
                    }
                    if (data.containsKey("service_type")) {
                        Object serviceType = data.get("service_type");
                        bins.add(new Bin("service_type", serviceType != null ? serviceType.toString() : null));
                    }
                    if (data.containsKey("province")) {
                        Object province = data.get("province");
                        bins.add(new Bin("province", province != null ? province.toString() : null));
                    }
                    if (data.containsKey("region")) {
                        Object region = data.get("region");
                        bins.add(new Bin("region", region != null ? region.toString() : null));
                    }
                    
                    // Handle last_updated with original data type
                    Object lastUpdateObj = data.get("last_updated");
                    if (lastUpdateObj != null) {
                        if (lastUpdateObj instanceof Long) {
                            bins.add(new Bin("last_updated", (Long) lastUpdateObj));
                        } else if (lastUpdateObj instanceof Integer) {
                            bins.add(new Bin("last_updated", ((Integer) lastUpdateObj).longValue()));
                        } else {
                            bins.add(new Bin("last_updated", System.currentTimeMillis()));
                        }
                    } else {
                        bins.add(new Bin("last_updated", System.currentTimeMillis()));
                    }
                    
                    // Handle notes with binary data format
                    if (data.containsKey("notes")) {
                        Object notesObj = data.get("notes");
                        if (notesObj != null) {
                            if (notesObj instanceof String) {
                                // If notes is a string, try to decode it as hex
                                try {
                                    String hexString = (String) notesObj;
                                    byte[] notesBytes = hexStringToByteArray(hexString);
                                    bins.add(new Bin("notes", notesBytes));
                                } catch (Exception e) {
                                    logger.error("[{}] Error converting notes hex string to bytes: {}", prefix, e.getMessage());
                                    // Fallback to original string if conversion fails
                                    bins.add(new Bin("notes", notesObj.toString()));
                                }
                            } else if (notesObj instanceof byte[]) {
                                // If notes is already a byte array, use it directly
                                bins.add(new Bin("notes", (byte[]) notesObj));
                            } else {
                                // For other types, convert to string and then to bytes
                                bins.add(new Bin("notes", notesObj.toString().getBytes(StandardCharsets.UTF_8)));
                            }
                        }
                    }
                    
                    // Write to Aerospike with all bins
                    destinationClient.put(writePolicy, key, bins.toArray(new Bin[0]));
                    
                } catch (JsonProcessingException e) {
                    logger.error("[{}] Error parsing JSON message: {}", prefix, e.getMessage());
                    throw new RuntimeException("Failed to parse JSON message", e);
                } catch (Exception e) {
                    logger.error("[{}] Error processing message: {}", prefix, e.getMessage());
                    throw e;
                }
            } else {
                logger.error("[{}] Error: Record value is null", prefix);
            }
            
        } catch (Exception e) {
            logger.error("[{}] Error writing to Aerospike: {}", prefix, e.getMessage(), e);
            throw e;
        }
    }

    // Helper method to convert hex string to byte array
    private byte[] hexStringToByteArray(String hexString) {
        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
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