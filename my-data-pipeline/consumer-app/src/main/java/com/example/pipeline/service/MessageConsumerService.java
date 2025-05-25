package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.nio.charset.StandardCharsets;

public class MessageConsumerService {
    private final AerospikeClient destinationClient;
    private final WritePolicy writePolicy;
    private final String destinationNamespace;
    private final String setName;
    private final RateLimiter rateLimiter;
    private final ThreadPoolExecutor workers;
    private final String prefix;
    private final ObjectMapper objectMapper;

    public MessageConsumerService(
            AerospikeClient destinationClient,
            WritePolicy writePolicy,
            String destinationNamespace,
            String setName,
            RateLimiter rateLimiter,
            ThreadPoolExecutor workers,
            String prefix,
            RateControlService rateControlService) {
        this.destinationClient = destinationClient;
        this.writePolicy = writePolicy;
        this.destinationNamespace = destinationNamespace;
        this.setName = setName;
        this.rateLimiter = rateLimiter;
        this.workers = workers;
        this.prefix = prefix;
        this.objectMapper = new ObjectMapper();
    }

    public void processRecord(ConsumerRecord<byte[], byte[]> record) {
        // Acquire rate limiter permit before processing
        if (!rateLimiter.tryAcquire()) {
            System.out.printf("[%s] Rate limit exceeded, waiting for permit...%n", prefix);
            rateLimiter.acquire(); // Block until permit is available
        }

        int maxRetries = 3;
        int currentRetry = 0;
        boolean success = false;
        String recordKey = new String(record.key(), StandardCharsets.UTF_8);

        while (!success && currentRetry < maxRetries) {
            try {
                if (!validateConnection(destinationClient)) {
                    System.err.printf("Aerospike connection error for record %s: Connection is not valid%n", recordKey);
                    throw new RuntimeException("Aerospike connection is not valid");
                }

                if (record.key() == null || record.value() == null) {
                    System.err.printf("Invalid record %s: key or value is null%n", recordKey);
                    throw new IllegalArgumentException("Invalid record: key or value is null");
                }

                byte[] userId = record.key();
                Key destinationKey = new Key(destinationNamespace, setName, userId);

                String jsonString = new String(record.value(), StandardCharsets.UTF_8);
                Map<String, Object> data = objectMapper.readValue(jsonString, 
                    new TypeReference<Map<String, Object>>() {});

                if (!data.containsKey("personData") || !data.containsKey("lastUpdate")) {
                    System.err.printf("Invalid data format for record %s: missing required fields%n", recordKey);
                    throw new IllegalArgumentException("Invalid data format: missing required fields");
                }

                String personDataBase64 = (String) data.get("personData");
                byte[] personData = Base64.getDecoder().decode(personDataBase64);
                long lastUpdate = ((Number) data.get("lastUpdate")).longValue();

                Bin personBin = new Bin("personData", personData);
                Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
                Bin keyBin = new Bin("PK", userId);

                try {
                    destinationClient.put(writePolicy, destinationKey, keyBin, personBin, lastUpdateBin);
                    success = true;
                } catch (Exception e) {
                    if (isRetryableError(e)) {
                        System.err.printf("Retryable error for record %s: %s%n", recordKey, e.getMessage());
                        throw e;
                    } else {
                        System.err.printf("Non-retryable error for record %s: %s%n", recordKey, e.getMessage());
                        throw new RuntimeException("Non-retryable error during write: " + e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                currentRetry++;
                if (currentRetry < maxRetries) {
                    System.err.printf("Retry attempt %d for record %s: %s%n", 
                        currentRetry, recordKey, e.getMessage());
                    handleRetry(currentRetry, e);
                } else {
                    System.err.printf("Failed to process record %s after %d attempts: %s%n", 
                        recordKey, maxRetries, e.getMessage());
                    throw new RuntimeException("Failed to process record after " + maxRetries + " attempts: " + e.getMessage(), e);
                }
            }
        }
    }

    private boolean validateConnection(AerospikeClient client) {
        try {
            return client != null && client.isConnected();
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isRetryableError(Exception e) {
        return e instanceof com.aerospike.client.AerospikeException.Timeout ||
               e instanceof com.aerospike.client.AerospikeException.Connection;
    }

    private void handleRetry(int currentRetry, Exception e) {
        long backoffTime = calculateBackoffTime(currentRetry);
        System.err.printf("Waiting %d ms before retry attempt %d%n", backoffTime, currentRetry);
        try {
            Thread.sleep(backoffTime);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry", ie);
        }
    }

    private long calculateBackoffTime(int retryCount) {
        long baseDelay = 1000;
        long maxDelay = 10000;
        long delay = Math.min(baseDelay * (1L << retryCount), maxDelay);
        return delay + (long)(Math.random() * 1000);
    }

    public void shutdown() {
        if (workers != null) {
            workers.shutdown();
            try {
                if (!workers.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                    workers.shutdownNow();
                }
            } catch (InterruptedException e) {
                workers.shutdownNow();
            }
        }
    }
}
