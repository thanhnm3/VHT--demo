package com.norm;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.google.common.util.concurrent.RateLimiter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AProducer {
    private static final AtomicInteger messagesSentThisSecond = new AtomicInteger(0);
    private static final AtomicLong totalMessagesSent = new AtomicLong(0);
    private static final AtomicInteger errorCount = new AtomicInteger(0);
    private static ExecutorService executor;
    private static final int BATCH_SIZE = 500;
    private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(5);

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String aerospikeHost, int aerospikePort, String namespace, String setName,
                          String kafkaBroker, String kafkaTopic, int maxRetries) {
        AerospikeClient aerospikeClient = null;
        KafkaProducer<String, byte[]> kafkaProducer = null;

        try {
            // Initialize Aerospike client with improved configuration
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.timeout = 5000; // 5 second timeout
            clientPolicy.maxConnsPerNode = 300; // Increase connection pool
            aerospikeClient = new AerospikeClient(clientPolicy, aerospikeHost, aerospikePort);

            // Initialize Kafka producer with optimized settings
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072"); // 128KB batch size
            kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "5"); // Wait up to 5ms to batch
            kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");
            kafkaProps.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(maxRetries));
            kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728"); // 128MB buffer
            kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            kafkaProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760"); // 10MB

            kafkaProducer = new KafkaProducer<>(kafkaProps);

            // Initialize thread pool with custom configuration
            ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                workerPoolSize,
                workerPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100000),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executor = customExecutor;

            // Schedule metrics reporting
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                int currentRate = messagesSentThisSecond.getAndSet(0);
                long total = totalMessagesSent.get();
                int errors = errorCount.get();
                System.out.printf("Messages <---      : %d, Total: %d, Errors: %d, Active threads: %d%n",
                    currentRate, total, errors, customExecutor.getActiveCount());
            }, 0, 1, TimeUnit.SECONDS);

            // Start processing
            readDataFromAerospike(aerospikeClient, kafkaProducer, maxMessagesPerSecond, 
                                namespace, setName, kafkaTopic, maxRetries);

        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownGracefully(aerospikeClient, kafkaProducer);
        }
    }

    private static void readDataFromAerospike(AerospikeClient client, KafkaProducer<String, byte[]> producer,
                                            int maxMessagesPerSecond, String namespace, String setName,
                                            String kafkaTopic, int maxRetries) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.maxConcurrentNodes = 4;
        scanPolicy.recordsPerSecond = maxMessagesPerSecond;

        RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond );

        List<ProducerRecord<String, byte[]>> batch = new ArrayList<>(BATCH_SIZE);
        Object batchLock = new Object();
        CountDownLatch scanLatch = new CountDownLatch(1);

        try {
            System.out.println("Starting to read data from Aerospike...");
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                rateLimiter.acquire();

                executor.submit(() -> {
                    try {
                        if (!isValidRecord(record)) {
                            System.err.println("Warning: Invalid record structure for key: " + key.userKey);
                            return;
                        }

                        ProducerRecord<String, byte[]> kafkaRecord = createKafkaRecord(key, record, kafkaTopic);
                        
                        synchronized (batchLock) {
                            batch.add(kafkaRecord);
                            if (batch.size() >= BATCH_SIZE) {
                                sendBatch(producer, new ArrayList<>(batch), maxRetries);
                                batch.clear();
                            }
                        }

                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        System.err.println("Error processing record: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            });

            // Send any remaining records in the final batch
            synchronized (batchLock) {
                if (!batch.isEmpty()) {
                    sendBatch(producer, new ArrayList<>(batch), maxRetries);
                    batch.clear();
                }
            }

            System.out.println("Finished scanning data from Aerospike.");
        } catch (Exception e) {
            System.err.println("Error scanning data from Aerospike: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scanLatch.countDown();
        }

        try {
            scanLatch.await(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while waiting for scan to complete");
        }
    }

    private static boolean isValidRecord(com.aerospike.client.Record record) {
        return record != null && record.bins != null && 
               record.bins.containsKey("personData") && 
               record.bins.containsKey("lastUpdate");
    }

    private static ProducerRecord<String, byte[]> createKafkaRecord(Key key, com.aerospike.client.Record record, String kafkaTopic) {
        byte[] personData = (byte[]) record.getValue("personData");
        long lastUpdate = System.currentTimeMillis();

        String message = String.format("{\"personData\": \"%s\", \"lastUpdate\": %d}",
                Base64.getEncoder().encodeToString(personData), lastUpdate);

        return new ProducerRecord<>(
                kafkaTopic,
                key.userKey.toString(),
                message.getBytes(StandardCharsets.UTF_8)
        );
    }

    private static void sendBatch(KafkaProducer<String, byte[]> producer, 
                                List<ProducerRecord<String, byte[]>> batch,
                                int maxRetries) {
        CountDownLatch latch = new CountDownLatch(batch.size());
        AtomicInteger batchErrors = new AtomicInteger(0);

        for (ProducerRecord<String, byte[]> record : batch) {
            producer.send(record, (metadata, exception) -> {
                try {
                    if (exception != null) {
                        handleSendError(producer, record, exception, maxRetries, batchErrors);
                    } else {
                        messagesSentThisSecond.incrementAndGet();
                        totalMessagesSent.incrementAndGet();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            if (!latch.await(FLUSH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                System.err.println("Timeout waiting for batch to complete from producer");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while waiting for batch completion");
        }
    }

    private static void handleSendError(KafkaProducer<String, byte[]> producer,
                                      ProducerRecord<String, byte[]> record,
                                      Exception exception,
                                      int maxRetries,
                                      AtomicInteger batchErrors) {
        int retryCount = 0;
        while (retryCount < maxRetries) {
            try {
                producer.send(record).get(5, TimeUnit.SECONDS);
                return;
            } catch (Exception retryEx) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    System.err.println("Failed to send message after " + maxRetries + 
                                     " retries: " + exception.getMessage());
                    errorCount.incrementAndGet();
                    batchErrors.incrementAndGet();
                    break;
                }
                try {
                    Thread.sleep(100 * retryCount); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void shutdownGracefully(AerospikeClient aerospikeClient, 
                                         KafkaProducer<String, byte[]> kafkaProducer) {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                    System.err.println("Executor did not terminate in time. Forcing shutdown...");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (kafkaProducer != null) {
            try {
                kafkaProducer.flush();
                kafkaProducer.close(Duration.ofSeconds(30));
                System.out.println("Kafka Producer closed successfully.");
            } catch (Exception e) {
                System.err.println("Error closing Kafka producer: " + e.getMessage());
            }
        }

        if (aerospikeClient != null) {
            try {
                aerospikeClient.close();
                System.out.println("Aerospike Client closed successfully.");
            } catch (Exception e) {
                System.err.println("Error closing Aerospike client: " + e.getMessage());
            }
        }
    }
}

