package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.admin.AdminClient;
import com.example.pipeline.service.RateControlService;
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
    private static AdminClient adminClient;
    private static String consumerGroup;
    private static final int MONITORING_INTERVAL_SECONDS = 10;
    private static final ScheduledExecutorService rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();
    private static RateControlService rateControlService;
    private static KafkaProducerService kafkaService;
    private static MessageProducerService messageService;
    private static AerospikeProducerService aerospikeService;

    private static final Map<String, String> prefixToTopicMap = new ConcurrentHashMap<>();
    private static String sourceNamespace;

    public static void main(String[] args) {
        try {
            // Lấy thông tin cấu hình từ args
            String kafkaBroker = args[0];
            String aerospikeHost = args[1];
            int aerospikePort = Integer.parseInt(args[2]);
            String namespace = args[3];
            String setName = args[4];
            int maxRetries = Integer.parseInt(args[5]);
            String consumerGroup = args[6];
            int workerPoolSize = Integer.parseInt(args[7]);
            String topicList = args[8]; // Danh sách topic được phân tách bằng dấu phẩy

            AProducer.consumerGroup = consumerGroup;
            AProducer.sourceNamespace = namespace;
            
            // Khởi tạo topic mapping từ danh sách topic được truyền vào
            initializeTopicMapping(topicList);
            
            AerospikeClient aerospikeClient = null;
            KafkaProducer<byte[], byte[]> kafkaProducer = null;

            try {
                rateControlService = new RateControlService(5000.0, MAX_RATE, MIN_RATE, 
                                                          LAG_THRESHOLD, MONITORING_INTERVAL_SECONDS);
                kafkaService = new KafkaProducerService(kafkaBroker, null, consumerGroup);
                messageService = new MessageProducerService();
                messageService.initializeTopicMapping(prefixToTopicMap);  

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
                    sourceNamespace
                );

                logger.info("Starting producer with configuration:");
                logger.info("  Namespace: {}", namespace);
                logger.info("  Set: {}", setName);
                logger.info("  Topics: {}", prefixToTopicMap);
                logger.info("  Consumer group: {}", consumerGroup);
                logger.info("  Worker pool size: {}", workerPoolSize);

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
        } catch (Exception e) {
            logger.error("Error in main: {}", e.getMessage(), e);
        }
    }

    private static void initializeTopicMapping(String topicList) {
        // Phân tách danh sách topic và tạo mapping từ prefix sang topic
        String[] topicArray = topicList.split(",");
        for (String topic : topicArray) {
            String trimmedTopic = topic.trim();
            // Lấy prefix từ topic name (ví dụ: từ "producer1_096-a" lấy "096")
            String prefix = trimmedTopic.split("_")[1].split("-")[0];
            prefixToTopicMap.put(prefix, trimmedTopic);
        }
        logger.info("Initialized topic mapping from prefix to topic: {}", prefixToTopicMap);
    }

    private static void createTopics() {
        try {
            Set<String> topics = new HashSet<>(prefixToTopicMap.values());
            
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
            if (consumerGroup == null) {
                logger.warn("Consumer group is not set, skipping lag monitoring");
                return;
            }

            KafkaLagMonitor lagMonitor = new KafkaLagMonitor();
            long totalLag = 0;
            boolean hasValidLag = false;
            
            // Tách consumer groups thành mảng
            String[] consumerGroups = consumerGroup.split(",");
            
            // Tạo map từ prefix sang topic và consumer group
            Map<String, String> prefixToTopicMap = new HashMap<>();
            Map<String, String> prefixToGroupMap = new HashMap<>();
            
            for (String group : consumerGroups) {
                group = group.trim();
                // Lấy prefix từ consumer group (ví dụ: từ "producer1_096-a-group" lấy "096")
                String prefix = group.split("_")[1].split("-")[0];
                String topic = "producer1_" + prefix + "-a";
                prefixToTopicMap.put(prefix, topic);
                prefixToGroupMap.put(prefix, group);
            }
            
            // Tính tổng lag cho mỗi cặp topic-group tương ứng
            for (Map.Entry<String, String> entry : prefixToTopicMap.entrySet()) {
                String prefix = entry.getKey();
                String topic = entry.getValue();
                String group = prefixToGroupMap.get(prefix);
                
                try {
                    String mirroredTopic = "source-kafka." + topic;
                    long topicLag = lagMonitor.calculateTopicLag(mirroredTopic, group);
                    
                    if (topicLag >= 0) {
                        totalLag += topicLag;
                        hasValidLag = true;
                        logger.info("Topic {} (mirrored as {}) has lag: {} for consumer group: {}", 
                                  topic, mirroredTopic, topicLag, group);
                    }
                } catch (Exception e) {
                    logger.warn("Error calculating lag for topic {} with consumer group {}: {}", 
                              topic, group, e.getMessage());
                }
            }

            if (hasValidLag) {
                double newRate = rateControlService.calculateNewRateForProducer(totalLag);
                rateControlService.updateRate(newRate);
                currentRate = rateControlService.getCurrentRate();
                logger.info("[Producer] Adjusted rate to {} messages/second based on total lag: {} for consumer groups: {}", 
                          String.format("%.2f", currentRate), totalLag, consumerGroup);
            } else {
                logger.warn("No valid lag found for any topic with consumer groups: {}. Keeping current rate: {}", 
                          consumerGroup, currentRate);
            }
                       
            lagMonitor.shutdown();
        } catch (Exception e) {
            logger.error("Error monitoring lag: {}", e.getMessage());
        }
    }

    private static void shutdownGracefully(AerospikeClient aerospikeClient, 
                                         KafkaProducer<byte[], byte[]> kafkaProducer) {
        logger.info("[SHUTDOWN] Starting graceful shutdown...");

        if (executor != null) {
            logger.info("[SHUTDOWN] Shutting down executor...");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                    logger.error("[SHUTDOWN] Executor did not terminate in time. Forcing shutdown...");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (rateControlService != null) {
            logger.info("[SHUTDOWN] Shutting down rate control service...");
            rateControlService.shutdown();
        }

        if (kafkaService != null) {
            logger.info("[SHUTDOWN] Shutting down Kafka service...");
            kafkaService.shutdown();
        }

        if (kafkaProducer != null) {
            try {
                logger.info("[SHUTDOWN] Flushing and closing Kafka producer...");
                // Process any pending messages first
                if (messageService != null) {
                    messageService.processPendingProducerMessages(kafkaProducer, 3);
                }
                // Flush all messages
                kafkaProducer.flush();
                // Close producer with timeout
                kafkaProducer.close(Duration.ofSeconds(30));
                logger.info("[SHUTDOWN] Kafka Producer closed successfully.");
            } catch (Exception e) {
                logger.error("[SHUTDOWN] Error closing Kafka producer: {}", e.getMessage());
            }
        }

        if (aerospikeClient != null) {
            try {
                logger.info("[SHUTDOWN] Closing Aerospike client...");
                aerospikeClient.close();
                logger.info("[SHUTDOWN] Aerospike Client closed successfully.");
            } catch (Exception e) {
                logger.error("[SHUTDOWN] Error closing Aerospike client: {}", e.getMessage());
            }
        }

        if (rateAdjustmentExecutor != null) {
            logger.info("[SHUTDOWN] Shutting down rate adjustment executor...");
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

        logger.info("[SHUTDOWN] Graceful shutdown completed.");
    }
}

