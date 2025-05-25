// package com.example.pipeline.service;

// import com.example.pipeline.model.ConsumerMetrics;
// import org.apache.kafka.clients.consumer.ConsumerRecords;

// public class ConsumerService {
//     private final ConsumerMetrics metrics;
//     private final MessageService messageService;
//     private volatile boolean isRunning = true;

//     public ConsumerService(ConsumerMetrics metrics, MessageService messageService) {
//         this.metrics = metrics;
//         this.messageService = messageService;
//     }

//     public void start() {
//         while (isRunning) {
//             try {
//                 // Poll for records
//                 ConsumerRecords<byte[], byte[]> records = metrics.getConsumer().poll(100);
                
//                 // Process records
//                 messageService.processRecords(records);
                
//                 // Monitor and adjust rate
//                 metrics.monitorAndAdjustLag();
                
//             } catch (Exception e) {
//                 System.err.println("Error in consumer service: " + e.getMessage());
//                 e.printStackTrace();
//             }
//         }
//     }

//     public void shutdown() {
//         isRunning = false;
//         metrics.shutdown();
//     }
// } 