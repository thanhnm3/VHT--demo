package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import com.example.pipeline.model.ConsumerMetrics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

public class MessageService {
    private final AerospikeClient destinationClient;
    private final WritePolicy writePolicy;
    private final ExecutorService workerPool;
    private final String namespace;
    private final String setName;
    private final ConsumerMetrics metrics;
    private volatile boolean isRunning = true;

    public MessageService(AerospikeClient destinationClient, WritePolicy writePolicy, 
                         String namespace, String setName, ConsumerMetrics metrics) {
        this.destinationClient = destinationClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.metrics = metrics;
        this.workerPool = Executors.newFixedThreadPool(metrics.getWorkerPoolSize());
    }

    public void start() {
        while (isRunning) {
            try {
                // Poll for records
                ConsumerRecords<byte[], byte[]> records = metrics.getConsumer().poll(Duration.ofMillis(100));
                
                // Process records
                processRecords(records);
                
                // Monitor and adjust rate
                metrics.monitorAndAdjustLag();
                
            } catch (Exception e) {
                System.err.println("Error in message service: " + e.getMessage());
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

            workerPool.submit(() -> {
                try {
                    processRecord(record);
                } catch (Exception e) {
                    System.err.println("Error processing record: " + e.getMessage());
                }
            });
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            // Create Aerospike key and bin
            Key key = new Key(namespace, setName, record.key());
            Bin valueBin = new Bin("value", record.value());
            
            // Write to Aerospike
            destinationClient.put(writePolicy, key, valueBin);
        } catch (Exception e) {
            System.err.println("Error writing to Aerospike: " + e.getMessage());
            throw e;
        }
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
        metrics.shutdown();
    }
} 