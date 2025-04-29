package com.norm;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


public class AProducer {
    private static ExecutorService executor;
    private static volatile double currentRate = 5000.0;
    private static final double MAX_RATE = 100000.0;
    private static final double MIN_RATE = 1000.0;
    private static final int LAG_THRESHOLD = 1000; // Ngưỡng lag để điều chỉnh tốc độ
    private static final AtomicLong producedCount = new AtomicLong(0);
    private static AdminClient adminClient;
    private static String consumerGroup;
    private static String kafkaTopic;
    private static final int MONITORING_INTERVAL_SECONDS = 10; // Thêm hằng số cho interval
    private static final AtomicLong lastRateAdjustmentTime = new AtomicLong(System.currentTimeMillis());
    private static final AtomicLong failedMessages = new AtomicLong(0);
    private static final AtomicLong skippedMessages = new AtomicLong(0);
    private static final AtomicLong timeoutMessages = new AtomicLong(0);
    private static final Queue<ProducerRecord<byte[], byte[]>> pendingMessages = new ConcurrentLinkedQueue<>();
    private static final int RATE_ADJUSTMENT_STEPS = 5; // Số bước điều chỉnh rate
    private static final double MAX_RATE_CHANGE_PERCENT = 0.2; // Tối đa 20% thay đổi mỗi lần
    private static volatile double targetRate = 5000.0; // Rate mục tiêu
    private static final ScheduledExecutorService rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String aerospikeHost, int aerospikePort, String namespace, String setName,
                          String kafkaBroker, String kafkaTopic, int maxRetries, String consumerGroup) {
        AProducer.consumerGroup = consumerGroup;
        AProducer.kafkaTopic = kafkaTopic;
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

            // Initialize Kafka Admin Client
            Properties adminProps = new Properties();
            adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            adminClient = AdminClient.create(adminProps);

            // Start lag monitoring thread
            new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - lastRateAdjustmentTime.get() >= MONITORING_INTERVAL_SECONDS * 1000) {
                            double oldRate = currentRate;
                            monitorAndAdjustLag();
                            if (oldRate != currentRate) {
                                System.out.printf("Rate adjusted from %.2f to %.2f messages/second%n", 
                                                oldRate, currentRate);
                                lastRateAdjustmentTime.set(currentTime);
                            }
                        }
                        Thread.sleep(1000); // Vẫn check mỗi giây nhưng chỉ điều chỉnh mỗi 10 giây
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }).start();

            // Initialize thread pool
            ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                workerPoolSize,
                workerPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100000),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executor = customExecutor;

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

    private static void adjustRateSmoothly(double newTargetRate) {
        if (newTargetRate == targetRate) return;

        // Tính toán số bước và lượng thay đổi cho mỗi bước
        final double rateChange = newTargetRate - currentRate;
        final double stepSize = rateChange / RATE_ADJUSTMENT_STEPS;
        
        // Giới hạn thay đổi tối đa mỗi bước
        final double maxStepChange = currentRate * MAX_RATE_CHANGE_PERCENT;
        final double finalStepSize = Math.abs(stepSize) > maxStepChange ? 
            Math.signum(stepSize) * maxStepChange : stepSize;

        // Lên lịch điều chỉnh rate từng bước
        for (int i = 0; i < RATE_ADJUSTMENT_STEPS; i++) {
            final int step = i;
            rateAdjustmentExecutor.schedule(() -> {
                double newRate = currentRate + finalStepSize;
                if (finalStepSize > 0) {
                    currentRate = Math.min(newRate, targetRate);
                } else {
                    currentRate = Math.max(newRate, targetRate);
                }
                System.out.printf("Rate adjusted to %.2f messages/second%n", currentRate);
            }, step * 1000, TimeUnit.MILLISECONDS); // Mỗi bước cách nhau 1 giây
        }

        targetRate = newTargetRate;
    }

    private static void monitorAndAdjustLag() {
        try {
            // Lấy thông tin consumer group
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(consumerGroup);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets = result.partitionsToOffsetAndMetadata().get();
            
            // Lấy thông tin topic partition
            List<TopicPartition> partitions = offsets.keySet().stream()
                .filter(tp -> tp.topic().equals(kafkaTopic))
                .collect(Collectors.toList());
                
            // Lấy end offset của topic
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient.listOffsets(
                partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))
            ).all().get();
            
            // Tính lag
            long totalLag = 0;
            for (TopicPartition partition : partitions) {
                long endOffset = endOffsets.get(partition).offset();
                long currentOffset = offsets.get(partition).offset();
                totalLag += (endOffset - currentOffset);
            }
            
            // Điều chỉnh tốc độ dựa trên lag
            if (totalLag > LAG_THRESHOLD) {
                double newRate = Math.max(MIN_RATE, currentRate * 0.9);
                adjustRateSmoothly(newRate);
            } else if (totalLag < LAG_THRESHOLD / 2) {
                double newRate = Math.min(MAX_RATE, currentRate * 1.1);
                adjustRateSmoothly(newRate);
            }
        } catch (Exception e) {
            System.err.println("Error monitoring lag: " + e.getMessage());
        }
    }

    // ======================= Read data from Aerospike =======================
    private static void readDataFromAerospike(AerospikeClient client, KafkaProducer<byte[], byte[]> producer,
                                            int maxMessagesPerSecond, String namespace, String setName,
                                            String kafkaTopic, int maxRetries) {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.maxConcurrentNodes = 4;
        scanPolicy.recordsPerSecond = (int) currentRate;

        RateLimiter rateLimiter = RateLimiter.create(currentRate);
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        Object batchLock = new Object();

        try {
            System.out.println("Starting to read data from Aerospike...");
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                rateLimiter.acquire();

                executor.submit(() -> {
                    try {
                        if (!isValidRecord(record)) {
                            String keyStr = key.userKey != null ? key.userKey.toString() : "null";
                            logSkippedMessage(keyStr, "Invalid record structure");
                            skippedMessages.incrementAndGet();
                            return;
                        }

                        ProducerRecord<byte[], byte[]> kafkaRecord = createKafkaRecord(key, record, kafkaTopic);
                        
                        synchronized (batchLock) {
                            if (currentRate < targetRate * 0.8) { // Nếu rate đang giảm mạnh
                                pendingMessages.offer(kafkaRecord); // Lưu vào queue
                            } else {
                                batch.add(kafkaRecord);
                                if (batch.size() >= 100) {
                                    sendBatch(producer, new ArrayList<>(batch), maxRetries);
                                    producedCount.addAndGet(batch.size());
                                    batch.clear();
                                }
                            }
                        }

                        // Xử lý message đang chờ khi có cơ hội
                        if (!pendingMessages.isEmpty() && currentRate >= targetRate * 0.9) {
                            processPendingMessages(producer, maxRetries);
                        }

                    } catch (Exception e) {
                        logFailedMessage(createKafkaRecord(key, record, kafkaTopic), "Processing error", e);
                        failedMessages.incrementAndGet();
                    }
                });
            });

            // Xử lý các message còn lại trong queue
            while (!pendingMessages.isEmpty()) {
                processPendingMessages(producer, maxRetries);
            }

            synchronized (batchLock) {
                if (!batch.isEmpty()) {
                    sendBatch(producer, new ArrayList<>(batch), maxRetries);
                    producedCount.addAndGet(batch.size());
                    batch.clear();
                }
            }

            System.out.println("Finished scanning data from Aerospike.");
            printMessageStats();
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

    private static void logFailedMessage(ProducerRecord<byte[], byte[]> record, String reason, Exception e) {
        String key = new String(record.key(), StandardCharsets.UTF_8);
        System.err.printf("[FAILED_MESSAGE] Key: %s, Reason: %s, Error: %s%n", 
                        key, reason, e != null ? e.getMessage() : "Unknown");
    }

    private static void logSkippedMessage(String key, String reason) {
        System.err.printf("[SKIPPED_MESSAGE] Key: %s, Reason: %s%n", key, reason);
    }

    private static void logTimeoutMessage(ProducerRecord<byte[], byte[]> record) {
        String key = new String(record.key(), StandardCharsets.UTF_8);
        System.err.printf("[TIMEOUT_MESSAGE] Key: %s, Reason: Batch send timeout%n", key);
    }

    private static void printMessageStats() {
        System.out.printf("\nMessage Statistics:%n" +
                         "Failed Messages: %d%n" +
                         "Skipped Messages: %d%n" +
                         "Timeout Messages: %d%n",
                         failedMessages.get(),
                         skippedMessages.get(),
                         timeoutMessages.get());
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

    private static void processPendingMessages(KafkaProducer<byte[], byte[]> producer, int maxRetries) {
        List<ProducerRecord<byte[], byte[]>> batch = new ArrayList<>(100);
        while (!pendingMessages.isEmpty() && batch.size() < 100) {
            ProducerRecord<byte[], byte[]> record = pendingMessages.poll();
            if (record != null) {
                batch.add(record);
            }
        }
        if (!batch.isEmpty()) {
            sendBatch(producer, batch, maxRetries);
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

        if (rateAdjustmentExecutor != null) {
            rateAdjustmentExecutor.shutdown();
            try {
                if (!rateAdjustmentExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                    rateAdjustmentExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                rateAdjustmentExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}

