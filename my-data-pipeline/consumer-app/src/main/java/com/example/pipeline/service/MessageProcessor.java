package com.example.pipeline.service;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Interface chung cho việc xử lý messages từ Kafka
 */
public interface MessageProcessor {
    
    /**
     * Xử lý batch records từ Kafka
     * @param records ConsumerRecords từ Kafka
     */
    void processRecords(ConsumerRecords<byte[], byte[]> records);
    
    /**
     * Xử lý batch records và đợi hoàn thành
     * @param records ConsumerRecords từ Kafka
     */
    void processRecordsAndWait(ConsumerRecords<byte[], byte[]> records);
    
    /**
     * Lấy current offset
     * @return current offset
     */
    long getCurrentOffset();
    
    /**
     * Lấy last processed offset
     * @return last processed offset
     */
    long getLastProcessedOffset();
    
    /**
     * Shutdown service
     */
    void shutdown();
} 