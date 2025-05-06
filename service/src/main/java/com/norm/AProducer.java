package com.norm;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import com.norm.service.RateControlService;
import com.norm.service.KafkaService;
import com.norm.service.MessageService;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class AProducer {
    private static ExecutorService executor;
    private static volatile double currentRate = 5000.0;
    private static final double MAX_RATE = 100000.0;
    private static final double MIN_RATE = 1000.0;
    private static final int LAG_THRESHOLD = 1000; // Ngưỡng lag để điều chỉnh tốc độ
    private static final AtomicLong producedCount = new AtomicLong(0);
    private static AdminClient adminClient;
    private static String consumerGroup;
    private static final int MONITORING_INTERVAL_SECONDS = 10; // Thêm hằng số cho interval
    private static final AtomicLong failedMessages = new AtomicLong(0);
    private static final AtomicLong skippedMessages = new AtomicLong(0);
    private static volatile double targetRate = 5000.0; // Rate mục tiêu
    private static final ScheduledExecutorService rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();
    private static RateControlService rateControlService;
    private static KafkaService kafkaService;
    private static MessageService messageService;

    // Định nghĩa các prefix
    private static final String[] PREFIXES = {"096", "033"};
    private static final Map<String, String> prefixToTopicMap = new ConcurrentHashMap<>();
    private static String defaultTopic;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String aerospikeHost, int aerospikePort, String namespace, String setName,
                          String kafkaBroker, int maxRetries, String consumerGroup) {
        AProducer.consumerGroup = consumerGroup;
        AProducer.defaultTopic = namespace + ".profile.default.produce";
        AerospikeClient aerospikeClient = null;
        KafkaProducer<byte[], byte[]> kafkaProducer = null;

        try {
            // Khởi tạo mapping prefix -> topic
            initializeTopicMapping(namespace);

            // Initialize services
            rateControlService = new RateControlService(5000.0, MAX_RATE, MIN_RATE, 
                                                      LAG_THRESHOLD, MONITORING_INTERVAL_SECONDS);
            kafkaService = new KafkaService(kafkaBroker, defaultTopic, consumerGroup);
            messageService = new MessageService();

            // Initialize Aerospike client
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.timeout = 5000;
            clientPolicy.maxConnsPerNode = 300;
            aerospikeClient = new AerospikeClient(clientPolicy, aerospikeHost, aerospikePort);

            // Initialize Kafka producer
            kafkaProducer = kafkaService.createProducer(maxRetries);

            // Initialize Kafka Admin Client
            Properties adminProps = new Properties();
            adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            adminClient = AdminClient.create(adminProps);

            // Tạo các topic trong Kafka
            createTopics();

            // Start lag monitoring thread
            new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (rateControlService.shouldCheckRateAdjustment()) {
                            double oldRate = currentRate;
                            monitorAndAdjustLag();
                            if (oldRate != currentRate) {
                                System.out.printf("Rate adjusted from %.2f to %.2f messages/second%n", 
                                                oldRate, currentRate);
                            }
                        }
                        Thread.sleep(1000);
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
                                namespace, setName, maxRetries);

        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownGracefully(aerospikeClient, kafkaProducer);
        }
    }

    // Khởi tạo mapping giữa prefix và topic
    private static void initializeTopicMapping(String namespace) {
        for (String prefix : PREFIXES) {
            String topicName = String.format("%s.profile.%s.produce", namespace, prefix);
            prefixToTopicMap.put(prefix, topicName);
        }
    }

    // Tạo các topic trong Kafka
    private static void createTopics() {
        try {
            Set<String> topics = new HashSet<>(prefixToTopicMap.values());
            topics.add(defaultTopic);
            
            // Tạo các topic mới nếu chưa tồn tại
            for (String topic : topics) {
                try {
                    kafkaService.createTopic(topic);
                    System.out.println("Created/Verified topic: " + topic);
                } catch (Exception e) {
                    System.err.println("Error creating topic " + topic + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Error creating topics: " + e.getMessage());
        }
    }

    // Phương thức xác định topic dựa trên prefix
    private static String determineTopic(byte[] key) {
        if (key == null || key.length < 3) {
            return defaultTopic;
        }
        
        String prefix = new String(key, 0, 3);
        return prefixToTopicMap.getOrDefault(prefix, defaultTopic);
    }

    private static void monitorAndAdjustLag() {
        try {
            long totalLag = kafkaService.calculateTotalLag();
            double newRate = rateControlService.calculateNewRateForProducer(totalLag);
            rateControlService.updateRate(newRate);
            currentRate = rateControlService.getCurrentRate();
        } catch (Exception e) {
            System.err.println("Error monitoring lag: " + e.getMessage());
        }
    }

    // ======================= Read data from Aerospike =======================
    private static void readDataFromAerospike(AerospikeClient client, KafkaProducer<byte[], byte[]> producer,
                                            int maxMessagesPerSecond, String namespace, String setName,
                                            int maxRetries) {
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
                            messageService.logSkippedMessage(keyStr, "Invalid record structure");
                            skippedMessages.incrementAndGet();
                            return;
                        }

                        ProducerRecord<byte[], byte[]> kafkaRecord = createKafkaRecord(key, record);
                        
                        synchronized (batchLock) {
                            if (currentRate < targetRate * 0.8) { // Nếu rate đang giảm mạnh
                                messageService.offerProducerMessage(kafkaRecord);
                            } else {
                                batch.add(kafkaRecord);
                                if (batch.size() >= 100) {
                                    messageService.sendBatch(producer, new ArrayList<>(batch), maxRetries);
                                    producedCount.addAndGet(batch.size());
                                    batch.clear();
                                }
                            }
                        }

                        // Xử lý message đang chờ khi có cơ hội
                        if (messageService.hasPendingProducerMessages() && currentRate >= targetRate * 0.9) {
                            messageService.processPendingProducerMessages(producer, maxRetries);
                        }

                    } catch (Exception e) {
                        messageService.logFailedMessage(createKafkaRecord(key, record), 
                                                      "Processing error", e);
                        failedMessages.incrementAndGet();
                    }
                });
            });

            // Xử lý các message còn lại trong queue
            while (messageService.hasPendingProducerMessages()) {
                messageService.processPendingProducerMessages(producer, maxRetries);
            }

            synchronized (batchLock) {
                if (!batch.isEmpty()) {
                    messageService.sendBatch(producer, new ArrayList<>(batch), maxRetries);
                    producedCount.addAndGet(batch.size());
                    batch.clear();
                }
            }

            System.out.println("Finished scanning data from Aerospike.");
            messageService.printMessageStats();
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

    private static ProducerRecord<byte[], byte[]> createKafkaRecord(Key key, com.aerospike.client.Record record) {
        byte[] personData = (byte[]) record.getValue("personData");
        long lastUpdate = System.currentTimeMillis();
        byte[] keyBytes = (byte[]) key.userKey.getObject();

        String message = String.format("{\"personData\": \"%s\", \"lastUpdate\": %d}",
                Base64.getEncoder().encodeToString(personData), lastUpdate);

        String topic = determineTopic(keyBytes);
        return new ProducerRecord<byte[], byte[]>(
                topic,
                keyBytes,
                message.getBytes(StandardCharsets.UTF_8)
        );
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

        if (rateControlService != null) {
            rateControlService.shutdown();
        }

        if (kafkaService != null) {
            kafkaService.shutdown();
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

