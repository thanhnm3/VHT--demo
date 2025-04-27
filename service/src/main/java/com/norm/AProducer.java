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


public class AProducer {
    private static ExecutorService executor;


    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String aerospikeHost, int aerospikePort, String namespace, String setName,
                          String kafkaBroker, String kafkaTopic, int maxRetries) {
        AerospikeClient aerospikeClient = null;
        KafkaProducer<byte[], byte[]> kafkaProducer = null;

        try {
            // Initialize Aerospike client
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.timeout = 5000;
            clientPolicy.maxConnsPerNode = 300;
            aerospikeClient = new AerospikeClient(clientPolicy, aerospikeHost, aerospikePort);

            // Initialize Kafka producer
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
            kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, "5");
            kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");
            kafkaProps.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(maxRetries));
            kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            kafkaProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");
            kafkaProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
            kafkaProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");

            kafkaProducer = new KafkaProducer<>(kafkaProps);

            // Initialize thread pool
            ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                workerPoolSize,
                workerPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100000),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executor = customExecutor;

            // Create RateLimiter
            // RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond);

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



    
    // ======================= Read data from Aerospike =======================
    private static void readDataFromAerospike(AerospikeClient client, KafkaProducer<byte[], byte[]> producer,
                                            int maxMessagesPerSecond, String namespace, String setName,
                                            String kafkaTopic, int maxRetries) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.maxConcurrentNodes = 4;
        scanPolicy.recordsPerSecond = maxMessagesPerSecond;

        RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond);
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        Object batchLock = new Object();

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

                        ProducerRecord<byte[], byte[]> kafkaRecord = createKafkaRecord(key, record, kafkaTopic);
                        
                        synchronized (batchLock) {
                            batch.add(kafkaRecord);
                            if (batch.size() >= 500) {
                                sendBatch(producer, new ArrayList<>(batch), maxRetries);
                                batch.clear();
                            }
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing record: " + e.getMessage());
                    }
                });
            });

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
        }
    }

    private static boolean isValidRecord(com.aerospike.client.Record record) {
        return record != null && record.bins != null && 
               record.bins.containsKey("personData") && 
               record.bins.containsKey("lastUpdate");
    }

    private static ProducerRecord<byte[], byte[]> createKafkaRecord(Key key, com.aerospike.client.Record record, String kafkaTopic) {
        byte[] personData = (byte[]) record.getValue("personData");
        long lastUpdate = System.currentTimeMillis();

        String message = String.format("{\"personData\": \"%s\", \"lastUpdate\": %d}",
                Base64.getEncoder().encodeToString(personData), lastUpdate);

        return new ProducerRecord<byte[], byte[]>(
                kafkaTopic,
                (byte[]) key.userKey.getObject(),
                message.getBytes(StandardCharsets.UTF_8)
        );
    }




    // ======================= Send batch =======================
    private static void sendBatch(KafkaProducer<byte[], byte[]> producer, 
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
                System.err.println("Timeout waiting for batch to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while waiting for batch completion");
        }
    }






    // ======================= Handle send error =======================
    private static void handleSendError(KafkaProducer<byte[], byte[]> producer,
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
                    System.err.println("Failed to send message after " + maxRetries + 
                                     " retries: " + exception.getMessage());
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





    // ======================= Shutdown gracefully =======================
    private static void shutdownGracefully(AerospikeClient aerospikeClient, 
                                         KafkaProducer<byte[], byte[]> kafkaProducer) {
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

