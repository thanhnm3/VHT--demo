package com.example.pipeline.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MessageService {
    private final Queue<ProducerRecord<byte[], byte[]>> producerPendingMessages;
    private final Queue<ConsumerRecord<byte[], byte[]>> consumerPendingMessages;
    private final AtomicLong failedMessages;
    private final AtomicLong skippedMessages;
    private final AtomicLong timeoutMessages;

    public MessageService() {
        this.producerPendingMessages = new ConcurrentLinkedQueue<>();
        this.consumerPendingMessages = new ConcurrentLinkedQueue<>();
        this.failedMessages = new AtomicLong(0);
        this.skippedMessages = new AtomicLong(0);
        this.timeoutMessages = new AtomicLong(0);
    }

    // ======================= Producer Methods =======================
    public void sendBatch(KafkaProducer<byte[], byte[]> producer, 
                         List<ProducerRecord<byte[], byte[]>> batch,
                         int maxRetries) {
        CountDownLatch latch = new CountDownLatch(batch.size());

        for (ProducerRecord<byte[], byte[]> record : batch) {
            producer.send(record, (metadata, exception) -> {
                try {
                    if (exception != null) {
                        handleSendError(producer, record, exception, maxRetries);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                for (ProducerRecord<byte[], byte[]> record : batch) {
                    logTimeoutMessage(record);
                    timeoutMessages.incrementAndGet();
                }
                System.err.println("Timeout waiting for batch to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while waiting for batch completion");
        }
    }

    public void processPendingProducerMessages(KafkaProducer<byte[], byte[]> producer, int maxRetries) {
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        while (!producerPendingMessages.isEmpty() && batch.size() < 100) {
            ProducerRecord<byte[], byte[]> record = producerPendingMessages.poll();
            if (record != null) {
                batch.add(record);
            }
        }
        if (!batch.isEmpty()) {
            sendBatch(producer, batch, maxRetries);
        }
    }

    public void offerProducerMessage(ProducerRecord<byte[], byte[]> record) {
        producerPendingMessages.offer(record);
    }

    // ======================= Consumer Methods =======================
    public ConsumerRecords<byte[], byte[]> pollMessages(KafkaConsumer<byte[], byte[]> consumer) {
        return consumer.poll(Duration.ofMillis(10));
    }

    public void processPendingConsumerMessages(List<ConsumerRecord<byte[], byte[]>> batch) {
        for (ConsumerRecord<byte[], byte[]> record : batch) {
            consumerPendingMessages.offer(record);
        }
    }

    public ConsumerRecord<byte[], byte[]> pollConsumerMessage() {
        return consumerPendingMessages.poll();
    }

    // ======================= Common Methods =======================
    private void handleSendError(KafkaProducer<byte[], byte[]> producer,
                               ProducerRecord<byte[], byte[]> record,
                               Exception exception,
                               int maxRetries) {
        int retryCount = 0;
        while (retryCount < maxRetries) {
            try {
                producer.send(record).get(5, TimeUnit.SECONDS);
                return;
            } catch (Exception retryEx) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    logFailedMessage(record, "Max retries exceeded", exception);
                    failedMessages.incrementAndGet();
                    break;
                }
                try {
                    Thread.sleep(100 * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public void logFailedMessage(ProducerRecord<byte[], byte[]> record, String reason, Exception e) {
        String key = new String(record.key(), StandardCharsets.UTF_8);
        System.err.printf("[FAILED_MESSAGE] Key: %s, Reason: %s, Error: %s%n", 
                        key, reason, e != null ? e.getMessage() : "Unknown");
    }

    public void logSkippedMessage(String key, String reason) {
        System.err.printf("[SKIPPED_MESSAGE] Key: %s, Reason: %s%n", key, reason);
    }

    private void logTimeoutMessage(ProducerRecord<byte[], byte[]> record) {
        String key = new String(record.key(), StandardCharsets.UTF_8);
        System.err.printf("[TIMEOUT_MESSAGE] Key: %s, Reason: Batch send timeout%n", key);
    }

    public void printMessageStats() {
        System.out.printf("\nMessage Statistics:%n" +
                         "Failed Messages: %d%n" +
                         "Skipped Messages: %d%n" +
                         "Timeout Messages: %d%n",
                         failedMessages.get(),
                         skippedMessages.get(),
                         timeoutMessages.get());
    }

    public boolean hasPendingProducerMessages() {
        return !producerPendingMessages.isEmpty();
    }

    public boolean hasPendingConsumerMessages() {
        return !consumerPendingMessages.isEmpty();
    }
} 