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
import com.example.pipeline.service.AerospikeProducerService;
import com.example.pipeline.service.KafkaLagMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class AProducer {
    private static final Logger logger = LoggerFactory.getLogger(AProducer.class);
    private static ExecutorService executor;
    private static volatile double currentRate = 5000.0;
    private static final double MAX_RATE = 100000.0;
    private static final double MIN_RATE = 1000.0;
    private static final int LAG_THRESHOLD = 1000;
    @SuppressWarnings("unused")
    private static AdminClient adminClient;
    @SuppressWarnings("unused")
    private static String consumerGroup;
    private static final int MONITORING_INTERVAL_SECONDS = 10;
    private static final ScheduledExecutorService rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();
    private static RateControlService rateControlService;
    private static KafkaProducerService kafkaService;
    private static MessageProducerService messageService;
    private static AerospikeProducerService aerospikeService;

    private static final Map<String, String> prefixToTopicMap = new ConcurrentHashMap<>();
    private static String defaultTopic;
    private static String sourceNamespace;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String aerospikeHost, int aerospikePort, String namespace, String setName,
                          String kafkaBroker, int maxRetries, String consumerGroup) {
        AProducer.consumerGroup = consumerGroup;
        AProducer.sourceNamespace = namespace;
        
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
            aerospikeClient = new AerospikeClient(clientPolicy, aerospikeHost, aerospikePort);

            kafkaProducer = kafkaService.createProducer(maxRetries);

            Properties adminProps = new Properties();
            adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            adminClient = AdminClient.create(adminProps);

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
                workerPoolSize,
                workerPoolSize,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100000),
                new ThreadPoolExecutor.CallerRunsPolicy()
            );
            executor = customExecutor;

            aerospikeService = new AerospikeProducerService(
                executor,
                messageService,
                prefixToTopicMap,
                defaultTopic,
                sourceNamespace
            );

            aerospikeService.readDataFromAerospike(
                aerospikeClient,
                kafkaProducer,
                currentRate,
                setName,
                maxRetries
            );

            monitorThread.interrupt();

        } catch (Exception e) {
            logger.error("Critical error: {}", e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownGracefully(aerospikeClient, kafkaProducer);
        }
    }

    private static void initializeTopicMapping() {
        Map<String, String> generatedTopics = TopicGenerator.generateTopics();
        prefixToTopicMap.putAll(generatedTopics);
        defaultTopic = String.format("%s.profile.default.produce", sourceNamespace);
        logger.info("Initialized topic mapping: {}", prefixToTopicMap);
        logger.info("Default topic: {}", defaultTopic);
    }

    private static void createTopics() {
        try {
            Set<String> topics = new HashSet<>(prefixToTopicMap.values());
            topics.add(defaultTopic);
            
            for (String topic : topics) {
                try {
                    kafkaService.createTopic(topic);
                    logger.info("Created/Verified topic: {}", topic);
                } catch (Exception e) {
                    logger.error("Error creating topic {}: {}", topic, e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error("Error creating topics: {}", e.getMessage());
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
                logger.warn("No matching topic found for producer namespace: {}", sourceNamespace);
                return;
            }
            
            try {
                // Thử tính lag cho topic đã mirror trước
                String mirroredTopic = "source-kafka." + producerTopic;
                long topicLag = lagMonitor.calculateTopicLag(mirroredTopic, consumerGroup);
                
                if (topicLag >= 0) {
                    producerLag = topicLag;
                    hasValidLag = true;
                    logger.info("Producer topic {} (mirrored as {}) has lag: {}", 
                              producerTopic, mirroredTopic, topicLag);
                } else {
                    // Nếu không tìm thấy topic đã mirror, thử với topic gốc
                    topicLag = lagMonitor.calculateTopicLag(producerTopic, consumerGroup);
                    if (topicLag >= 0) {
                        producerLag = topicLag;
                        hasValidLag = true;
                        logger.info("Producer topic {} has lag: {}", producerTopic, topicLag);
                    }
                }
            } catch (Exception e) {
                logger.warn("Error calculating lag for producer topic {}: {}", producerTopic, e.getMessage());
            }

            // Chỉ điều chỉnh rate nếu có lag hợp lệ
            if (hasValidLag) {
                double newRate = rateControlService.calculateNewRateForProducer(producerLag);
                rateControlService.updateRate(newRate);
                currentRate = rateControlService.getCurrentRate();
                logger.info("[Producer] Adjusted rate to {} messages/second based on producer lag: {}", 
                          String.format("%.2f", currentRate), producerLag);
            } else {
                logger.warn("No valid lag found for producer topic. Keeping current rate: {}", currentRate);
            }
                       
            lagMonitor.shutdown();
        } catch (Exception e) {
            logger.error("Error monitoring lag: {}", e.getMessage());
        }
    }

    private static void shutdownGracefully(AerospikeClient aerospikeClient, 
                                         KafkaProducer<byte[], byte[]> kafkaProducer) {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                    logger.error("Executor did not terminate in time. Forcing shutdown...");
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
                logger.info("Kafka Producer closed successfully.");
            } catch (Exception e) {
                logger.error("Error closing Kafka producer: {}", e.getMessage());
            }
        }

        if (aerospikeClient != null) {
            try {
                aerospikeClient.close();
                logger.info("Aerospike Client closed successfully.");
            } catch (Exception e) {
                logger.error("Error closing Aerospike client: {}", e.getMessage());
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

