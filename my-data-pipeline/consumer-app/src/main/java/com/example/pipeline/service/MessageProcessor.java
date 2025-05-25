package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.example.pipeline.util.AerospikeUtils;
import com.example.pipeline.model.ConsumerMetrics;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;

public class MessageProcessor {
    private static final int MAX_EMPTY_POLLS = 100; // 100 * 100ms = 10s
    private static final int POLL_TIMEOUT_MS = 100;

    private static boolean shouldShutdown(int emptyPollCount) {
        if (emptyPollCount >= MAX_EMPTY_POLLS) {
            System.out.printf("No messages received for %d seconds. Initiating shutdown.%n", 
                            MAX_EMPTY_POLLS * POLL_TIMEOUT_MS / 1000);
            return true;
        }
        return false;
    }

    public static void processMessages(ConsumerMetrics metrics,
                                     AerospikeClient destinationClient,
                                     WritePolicy writePolicy) {
        int emptyPollCount = 0;

        try {
            while (!Thread.currentThread().isInterrupted() && metrics.isRunning()) {
                ConsumerRecords<byte[], byte[]> records = metrics.getConsumer().poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (!records.isEmpty()) {
                    emptyPollCount = 0; // reset if there are messages
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        try {
                            // Acquire rate limiter permit before processing
                            if (!metrics.getRateLimiter().tryAcquire()) {
                                System.out.printf("[%s] Rate limit exceeded, waiting for permit...%n", 
                                                metrics.getPrefix());
                                metrics.getRateLimiter().acquire(); // Block until permit is available
                            }

                            if (metrics.getCurrentRate() < metrics.getRateControlService().getTargetRate() * 0.8) {
                                AerospikeUtils.processRecord(record.key(), record.value(), 
                                                          destinationClient, writePolicy,
                                                          metrics.getTargetNamespace(), metrics.getSetName());
                            } else {
                                metrics.getWorkers().submit(() -> {
                                    try {
                                        // Acquire rate limiter permit in worker thread
                                        if (!metrics.getRateLimiter().tryAcquire()) {
                                            System.out.printf("[%s] Rate limit exceeded in worker thread, waiting for permit...%n", 
                                                            metrics.getPrefix());
                                            metrics.getRateLimiter().acquire(); // Block until permit is available
                                        }
                                        AerospikeUtils.processRecord(record.key(), record.value(),
                                                                   destinationClient, writePolicy,
                                                                   metrics.getTargetNamespace(), metrics.getSetName());
                                    } catch (Exception e) {
                                        System.err.printf("[%s] Error processing record: %s%n", 
                                                        metrics.getPrefix(), e.getMessage());
                                    }
                                });
                            }
                        } catch (Exception e) {
                            System.err.printf("[%s] Error in message processing: %s%n", 
                                            metrics.getPrefix(), e.getMessage());
                        }
                    }
                    
                    // Commit offsets after successful batch processing
                    try {
                        metrics.getConsumer().commitSync();
                    } catch (Exception e) {
                        System.err.printf("[%s] Error committing offsets: %s%n", 
                                        metrics.getPrefix(), e.getMessage());
                    }
                } else {
                    emptyPollCount++;
                    if (shouldShutdown(emptyPollCount)) {
                        metrics.shutdown();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            if (metrics.isRunning()) {
                System.err.printf("[%s] Error in message processing: %s%n", 
                                metrics.getPrefix(), e.getMessage());
                e.printStackTrace();
            }
        }
    }
} 