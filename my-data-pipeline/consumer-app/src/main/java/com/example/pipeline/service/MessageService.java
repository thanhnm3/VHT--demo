package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class MessageService {
    private final AerospikeClient destinationClient;
    private final WritePolicy writePolicy;
    private final ExecutorService workerPool;
    private final String namespace;
    private final String setName;
    private final String prefix;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final RateLimiter rateLimiter;
    private final RateControlService rateControlService;
    private volatile double currentRate;
    private volatile boolean isRunning = true;
    private final AtomicLong lastProcessedOffset = new AtomicLong(0);
    private final AtomicLong currentOffset = new AtomicLong(0);
    private final AtomicLong processedRecords = new AtomicLong(0);

    public MessageService(AerospikeClient destinationClient, WritePolicy writePolicy, 
                         String namespace, String setName, String prefix,
                         KafkaConsumer<byte[], byte[]> consumer, int workerPoolSize) {
        this.destinationClient = destinationClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.prefix = prefix;
        this.consumer = consumer;
        this.currentRate = 8000.0;
        this.rateLimiter = RateLimiter.create(currentRate);
        this.rateControlService = new RateControlService(currentRate, 10000.0, 2000.0, 1000, 10);
        
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

        // Start rate monitor thread
        startRateMonitor();
    }

    private void startRateMonitor() {
        Thread monitorThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && isRunning) {
                try {
                    if (rateControlService.shouldCheckRateAdjustment()) {
                        double oldRate = currentRate;
                        double newRate = rateControlService.calculateNewRateForConsumer(
                            currentOffset.get(), lastProcessedOffset.get());
                        
                        if (newRate != oldRate) {
                            rateControlService.updateRate(newRate);
                            currentRate = rateControlService.getCurrentRate();
                            rateLimiter.setRate(currentRate);
                            System.out.printf("[%s] Rate adjusted from %.2f to %.2f messages/second%n", 
                                            prefix, oldRate, currentRate);
                        }
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, prefix + "-rate-monitor");
        monitorThread.start();
    }

    public void start() {
        while (isRunning) {
            try {
                // Poll for records
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                
                // Update current offset
                if (!records.isEmpty()) {
                    records.forEach(record -> {
                        currentOffset.set(record.offset());
                    });
                }
                
                // Process records
                processRecords(records);
                
            } catch (Exception e) {
                System.err.printf("[%s] Error in message service: %s%n", 
                                prefix, e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void processRecords(ConsumerRecords<byte[], byte[]> records) {
        if (!isRunning || records.isEmpty()) return;

        List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            recordList.add(record);
        }

        processBatch(recordList);
    }

    private void processBatch(List<ConsumerRecord<byte[], byte[]>> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (!isRunning) break;

            // Apply rate limiting
            if (rateControlService.getCurrentRate() > 0) {
                rateLimiter.acquire();
            }

            workerPool.submit(() -> {
                try {
                    processRecord(record);
                    // Update last processed offset after successful processing
                    lastProcessedOffset.set(record.offset());
                    processedRecords.incrementAndGet();
                } catch (Exception e) {
                    System.err.printf("[%s] Error processing record: %s%n", 
                                    prefix, e.getMessage());
                }
            });
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            // Create Aerospike key using record key as PK
            Key key = new Key(namespace, setName, record.key());
            
            // Parse message similar to AProducer
            String message = new String(record.value(), StandardCharsets.UTF_8);
            String[] parts = message.split("\"personData\": \"");
            String personDataBase64 = parts[1].split("\"")[0];
            String lastUpdateStr = message.split("\"lastUpdate\": ")[1].split("}")[0];
            
            // Decode personData from base64
            byte[] personData = Base64.getDecoder().decode(personDataBase64);
            long lastUpdate = Long.parseLong(lastUpdateStr);
            
            // Create separate bins for personData and lastUpdate
            Bin personDataBin = new Bin("personData", personData);
            Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
            
            // Write to Aerospike with both bins
            destinationClient.put(writePolicy, key, personDataBin, lastUpdateBin);
        } catch (Exception e) {
            System.err.printf("[%s] Error writing to Aerospike: %s%n", 
                            prefix, e.getMessage());
            throw e;
        }
    }

    public void shutdown() {
        isRunning = false;
        if (rateControlService != null) {
            rateControlService.shutdown();
        }
        if (consumer != null) {
            consumer.wakeup();
            consumer.close();
        }
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
        }
    }
} 