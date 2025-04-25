package com.norm;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.util.concurrent.RateLimiter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Base64;
import java.util.Collections;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AConsumer {
    private static final AtomicInteger messagesProcessedThisSecond = new AtomicInteger(0);
    private static final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private static final AtomicInteger errorCount = new AtomicInteger(0);
    private static ExecutorService executor;
    private static final int BATCH_SIZE = 200;
    private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(10);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final AtomicLong totalProcessingTime = new AtomicLong(0);
    private static final AtomicInteger batchCount = new AtomicInteger(0);

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String sourceHost, int sourcePort, String sourceNamespace,
                          String destinationHost, int destinationPort, String destinationNamespace,
                          String setName, String kafkaBroker, String kafkaTopic, String consumerGroup) {
        KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
        AerospikeClient destinationClient = null;

        try {
            // Initialize Aerospike client with improved configuration
            destinationClient = new AerospikeClient(destinationHost, destinationPort);
            WritePolicy writePolicy = new WritePolicy();

            // Initialize Kafka consumer with optimized settings
            Properties kafkaProps = new Properties();
            kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            kafkaProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
            kafkaProps.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "52428800"); // 50MB
            kafkaProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "16384");
            kafkaProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
            kafkaProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10485760");
            kafkaProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

            kafkaConsumer = new KafkaConsumer<>(kafkaProps);
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            // Create RateLimiter with some overhead allowance
            RateLimiter rateLimiter = RateLimiter.create(maxMessagesPerSecond + 300);

            // Initialize thread pool with custom configuration
            ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                workerPoolSize,
                workerPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(10000),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executor = customExecutor;

            // Schedule metrics reporting
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                int currentRate = messagesProcessedThisSecond.getAndSet(0);
                long total = totalMessagesProcessed.get();
                int errors = errorCount.get();
                long avgProcessingTime = batchCount.get() > 0 ? 
                    totalProcessingTime.get() / batchCount.get() / 1_000_000 : 0; // ms
                
                System.out.printf("Messages --->      : %d/s, Total: %d, Errors: %d, " +
                                 "Active threads: %d, Avg processing time: %dms%n",
                    currentRate, total, errors, customExecutor.getActiveCount(), avgProcessingTime);
            }, 0, 1, TimeUnit.SECONDS);

            // Start processing
            processKafkaMessages(kafkaConsumer, destinationClient, writePolicy, 
                               destinationNamespace, setName, rateLimiter);

        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownGracefully(kafkaConsumer, destinationClient);
        }
    }

    private static void processKafkaMessages(KafkaConsumer<byte[], byte[]> consumer,
                                           AerospikeClient destinationClient,
                                           WritePolicy writePolicy,
                                           String destinationNamespace,
                                           String setName,
                                           RateLimiter rateLimiter) {
        List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>(BATCH_SIZE);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Lên lịch gửi batch mỗi giây
        scheduler.scheduleAtFixedRate(() -> {
            List<ConsumerRecord<byte[], byte[]>> currentBatch;
            synchronized (batch) {
                if (batch.isEmpty()) return;
                currentBatch = new ArrayList<>(batch);
                batch.clear();
            }
            
            // Xử lý batch ngay lập tức
            processBatch(destinationClient, writePolicy, currentBatch, 
                       destinationNamespace, setName);
        }, 0, 1, TimeUnit.SECONDS);
        
        try {
            while (true) {
                try {
                    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) continue;

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        rateLimiter.acquire(); // Giới hạn tốc độ theo MAX_MESSAGES_PER_SECOND
                        
                        synchronized (batch) {
                            batch.add(record);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Error in message processing loop: " + e.getMessage());
                    e.printStackTrace();
                    continue;
                }
            }
        } catch (Exception e) {
            System.err.println("Fatal error in Kafka message processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void processBatch(AerospikeClient destinationClient,
                                   WritePolicy writePolicy,
                                   List<ConsumerRecord<byte[], byte[]>> batch,
                                   String destinationNamespace,
                                   String setName) {
        if (batch.isEmpty()) return;

        long startTime = System.nanoTime();
        CountDownLatch latch = new CountDownLatch(batch.size());
        List<ConsumerRecord<byte[], byte[]>> unprocessedRecords = new ArrayList<>();

        try {
            // Sử dụng số lượng thread được cung cấp
            int workerPoolSize = executor instanceof ThreadPoolExecutor ? 
                ((ThreadPoolExecutor) executor).getMaximumPoolSize() : 1;
            
            // Tính toán kích thước sub-batch
            int recordsPerThread = Math.max(1, batch.size() / workerPoolSize);
            
            // Tạo danh sách các sub-batch
            List<List<ConsumerRecord<byte[], byte[]>>> subBatches = new ArrayList<>(workerPoolSize);
            for (int i = 0; i < workerPoolSize; i++) {
                int start = i * recordsPerThread;
                int end = Math.min(start + recordsPerThread, batch.size());
                if (start < end) {
                    subBatches.add(batch.subList(start, end));
                }
            }

            // Xử lý song song các sub-batch
            for (List<ConsumerRecord<byte[], byte[]>> subBatch : subBatches) {
                executor.submit(() -> {
                    try {
                        for (ConsumerRecord<byte[], byte[]> record : subBatch) {
                            try {
                                processRecord(record, destinationClient, writePolicy, 
                                           destinationNamespace, setName, latch);
                            } catch (Exception e) {
                                System.err.println("Error processing record: " + e.getMessage());
                                e.printStackTrace();
                                synchronized (unprocessedRecords) {
                                    unprocessedRecords.add(record);
                                }
                                latch.countDown();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing sub-batch: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            }

            // Đợi tất cả các record được xử lý
            if (!latch.await(FLUSH_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                System.err.println("Timeout waiting for batch to complete");
                // Lấy các record chưa được xử lý
                long remaining = latch.getCount();
                System.err.println("Unprocessed records: " + remaining);
                
                // Xử lý các record còn lại trong main thread
                if (remaining > 0) {
                    System.err.println("Processing remaining records in main thread...");
                    for (ConsumerRecord<byte[], byte[]> record : batch) {
                        if (!unprocessedRecords.contains(record)) {
                            try {
                                processRecord(record, destinationClient, writePolicy, 
                                           destinationNamespace, setName, new CountDownLatch(1));
                            } catch (Exception e) {
                                System.err.println("Error processing remaining record: " + e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error in batch processing: " + e.getMessage());
            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        totalProcessingTime.addAndGet(endTime - startTime);
        batchCount.incrementAndGet();
    }

    private static void processRecord(ConsumerRecord<byte[], byte[]> record,
                                    AerospikeClient destinationClient,
                                    WritePolicy writePolicy,
                                    String destinationNamespace,
                                    String setName,
                                    CountDownLatch latch) {
        int retryCount = 0;
        final int MAX_RETRIES = 3;
        
        while (retryCount < MAX_RETRIES) {
            try {
                String userId = new String(record.key());
                Key destinationKey = new Key(destinationNamespace, setName, userId);

                String jsonString = new String(record.value(), StandardCharsets.UTF_8);
                Map<String, Object> data = objectMapper.readValue(jsonString, 
                    new TypeReference<Map<String, Object>>() {});

                String personDataBase64 = (String) data.get("personData");
                byte[] personData = Base64.getDecoder().decode(personDataBase64);
                long lastUpdate = ((Number) data.get("lastUpdate")).longValue();

                Bin personBin = new Bin("personData", personData);
                Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
                Bin keyBin = new Bin("PK", userId);

                destinationClient.put(writePolicy, destinationKey, keyBin, personBin, lastUpdateBin);
                
                messagesProcessedThisSecond.incrementAndGet();
                totalMessagesProcessed.incrementAndGet();
                break; // Thoát khỏi vòng lặp nếu thành công
            } catch (Exception e) {
                retryCount++;
                if (retryCount >= MAX_RETRIES) {
                    System.err.println("Failed to process record after " + MAX_RETRIES + 
                                     " retries: " + e.getMessage());
                    errorCount.incrementAndGet();
                    break;
                }
                try {
                    Thread.sleep(100 * (1 << (retryCount - 1))); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        latch.countDown();
    }

    private static void shutdownGracefully(KafkaConsumer<byte[], byte[]> kafkaConsumer,
                                         AerospikeClient aerospikeClient) {
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

        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.close();
                System.out.println("Kafka Consumer closed successfully.");
            } catch (Exception e) {
                System.err.println("Error closing Kafka consumer: " + e.getMessage());
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
