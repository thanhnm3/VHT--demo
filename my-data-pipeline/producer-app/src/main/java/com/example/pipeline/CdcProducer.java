package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import com.example.pipeline.service.RateControlService;
import com.example.pipeline.service.TopicGenerator;
import com.example.pipeline.service.KafkaProducerService;
import com.example.pipeline.service.MessageProducerService;
import com.example.pipeline.service.CdcProducerService;
import com.example.pipeline.service.KafkaLagMonitor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class CdcProducer {
    private static ExecutorService executor;
    private static volatile double currentRate = 5000.0;
    private static final double MAX_RATE = 100000.0;
    private static final double MIN_RATE = 1000.0;
    private static final int LAG_THRESHOLD = 1000;
    private static AdminClient adminClient;
    private static String consumerGroup;
    private static final int MONITORING_INTERVAL_SECONDS = 5;
    private static final ScheduledExecutorService rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();
    private static RateControlService rateControlService;
    private static KafkaProducerService kafkaService;
    private static MessageProducerService messageService;
    private static CdcProducerService cdcProducerService;

    private static final Map<String, String> prefixToTopicMap = new ConcurrentHashMap<>();
    private static String defaultTopic;
    private static String sourceNamespace;

    public static void start(String aeroHost, int aeroPort, String namespace, String setName,
            String kafkaBroker, int maxRetries, int threadPoolSize, String consumerGroup) {
        sourceNamespace = namespace;
        CdcProducer.consumerGroup = consumerGroup;
        initializeTopicMapping();
        
        AerospikeClient aerospikeClient = null;
        KafkaProducer<byte[], byte[]> kafkaProducer = null;

        try {
            rateControlService = new RateControlService(5000.0, MAX_RATE, MIN_RATE, 
                                                      LAG_THRESHOLD, MONITORING_INTERVAL_SECONDS);
            kafkaService = new KafkaProducerService(kafkaBroker, defaultTopic, consumerGroup);
            messageService = new MessageProducerService();

            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.timeout = 5000;
            clientPolicy.maxConnsPerNode = 300;
            aerospikeClient = new AerospikeClient(clientPolicy, aeroHost, aeroPort);

            kafkaProducer = kafkaService.createProducer(maxRetries);

            Properties adminProps = new Properties();
            adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            adminClient = AdminClient.create(adminProps);

            // Hiển thị thông tin cấu hình tĩnh
            System.out.println("\n=== CDC Producer Configuration ===");
            System.out.println("Kafka Broker: " + kafkaBroker);
            System.out.println("Aerospike Host: " + aeroHost);
            System.out.println("Aerospike Port: " + aeroPort);
            System.out.println("Namespace: " + namespace);
            System.out.println("Set Name: " + setName);
            System.out.println("Thread Pool Size: " + threadPoolSize);
            System.out.println("Max Retries: " + maxRetries);
            System.out.println("Initial Rate: " + currentRate);
            System.out.println("Max Rate: " + MAX_RATE);
            System.out.println("Min Rate: " + MIN_RATE);
            System.out.println("Lag Threshold: " + LAG_THRESHOLD);
            System.out.println("\nTopic Mapping:");
            for (Map.Entry<String, String> entry : prefixToTopicMap.entrySet()) {
                String prefix = entry.getKey();
                String topic = entry.getValue();
                System.out.printf("  Prefix: %s\n", prefix);
                System.out.printf("    Topic: %s\n", topic);
            }
            System.out.println("Default Topic: " + defaultTopic);
            System.out.println("===============================\n");

            createTopics();

            Thread monitorThread = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        if (rateControlService.shouldCheckRateAdjustment()) {
                            monitorAndAdjustLag();
                        }
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            monitorThread.start();

            ThreadPoolExecutor customExecutor = new ThreadPoolExecutor(
                threadPoolSize,
                threadPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100000),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executor = customExecutor;

            cdcProducerService = new CdcProducerService(
                executor,
                messageService,
                prefixToTopicMap,
                defaultTopic,
                sourceNamespace
            );

            cdcProducerService.readDataFromAerospike(
                aerospikeClient,
                kafkaProducer,
                currentRate,
                setName,
                maxRetries
            );

            monitorThread.interrupt();

        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownGracefully(aerospikeClient, kafkaProducer);
        }
    }

    private static void initializeTopicMapping() {
        Map<String, String> generatedTopics = TopicGenerator.generateTopics();
        // Chuyển đổi các topic thành topic CDC
        for (Map.Entry<String, String> entry : generatedTopics.entrySet()) {
            String cdcTopic = TopicGenerator.generateCdcTopicName(entry.getValue());
            prefixToTopicMap.put(entry.getKey(), cdcTopic);
        }
        defaultTopic = String.format("%s.profile.default.cdc", sourceNamespace);
        System.out.println("Initialized CDC topic mapping: " + prefixToTopicMap);
        System.out.println("Default CDC topic: " + defaultTopic);
    }

    private static void createTopics() {
        try {
            Set<String> topics = new HashSet<>(prefixToTopicMap.values());
            topics.add(defaultTopic);
            
            for (String topic : topics) {
                try {
                    kafkaService.createTopic(topic);
                    System.out.println("Created/Verified CDC topic: " + topic);
                } catch (Exception e) {
                    System.err.println("Error creating CDC topic " + topic + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Error creating CDC topics: " + e.getMessage());
        }
    }

    private static void monitorAndAdjustLag() {
        try {
            // Khởi tạo KafkaLagMonitor với target broker
            KafkaLagMonitor lagMonitor = new KafkaLagMonitor();
            
            // Chỉ tính lag cho topic của producer này
            long producerLag = 0;
            boolean hasValidLag = false;
            
            // Lấy topic của producer này từ prefix mapping
            String producerTopic = null;
            for (Map.Entry<String, String> entry : prefixToTopicMap.entrySet()) {
                if (entry.getValue().contains(sourceNamespace)) {
                    producerTopic = entry.getValue();
                    break;
                }
            }
            
            if (producerTopic == null) {
                System.err.println("No matching topic found for producer namespace: " + sourceNamespace);
                return;
            }
            
            try {
                // Thử tính lag cho topic đã mirror trước
                String mirroredTopic = "source-kafka." + producerTopic;
                long topicLag = lagMonitor.calculateTopicLag(mirroredTopic, consumerGroup);
                
                if (topicLag >= 0) {
                    producerLag = topicLag;
                    hasValidLag = true;
                    System.out.printf("Producer topic %s (mirrored as %s) has lag: %d%n", 
                              producerTopic, mirroredTopic, topicLag);
                } else {
                    // Nếu không tìm thấy topic đã mirror, thử với topic gốc
                    topicLag = lagMonitor.calculateTopicLag(producerTopic, consumerGroup);
                    if (topicLag >= 0) {
                        producerLag = topicLag;
                        hasValidLag = true;
                        System.out.printf("Producer topic %s has lag: %d%n", producerTopic, topicLag);
                    }
                }
            } catch (Exception e) {
                System.err.printf("Error calculating lag for producer topic %s: %s%n", 
                                producerTopic, e.getMessage());
            }

            // Chỉ điều chỉnh rate nếu có lag hợp lệ
            if (hasValidLag) {
                double newRate = rateControlService.calculateNewRateForProducer(producerLag);
                rateControlService.updateRate(newRate);
                currentRate = rateControlService.getCurrentRate();
                System.out.printf("[CDC Producer] Adjusted rate to %.2f messages/second based on producer lag: %d%n", 
                              currentRate, producerLag);
            } else {
                System.out.printf("No valid lag found for producer topic. Keeping current rate: %.2f%n", currentRate);
            }
                       
            lagMonitor.shutdown();
        } catch (Exception e) {
            System.err.println("Error monitoring lag: " + e.getMessage());
        }
    }

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

        if (cdcProducerService != null) {
            cdcProducerService.shutdown();
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
