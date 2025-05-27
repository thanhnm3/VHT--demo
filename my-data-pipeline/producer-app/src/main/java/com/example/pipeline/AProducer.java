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

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class AProducer {
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
                            double oldRate = currentRate;
                            monitorAndAdjustLag();
                            if (oldRate != currentRate) {
                                System.out.printf("[Producer] Rate adjusted from %.2f to %.2f messages/second%n", 
                                                oldRate, currentRate);
                            }
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
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownGracefully(aerospikeClient, kafkaProducer);
        }
    }

    private static void initializeTopicMapping() {
        Map<String, String> generatedTopics = TopicGenerator.generateTopics();
        prefixToTopicMap.putAll(generatedTopics);
        defaultTopic = String.format("%s.profile.default.produce", sourceNamespace);
        System.out.println("Initialized topic mapping: " + prefixToTopicMap);
        System.out.println("Default topic: " + defaultTopic);
    }

    private static void createTopics() {
        try {
            Set<String> topics = new HashSet<>(prefixToTopicMap.values());
            topics.add(defaultTopic);
            
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

    private static void monitorAndAdjustLag() {
        try {
            long totalLag = kafkaService.calculateTotalLag();
            double newRate = rateControlService.calculateNewRateForProducer(totalLag);
            rateControlService.updateRate(newRate);
            currentRate = rateControlService.getCurrentRate();
            System.out.printf("[Producer] Adjusted rate to %.2f messages/second based on lag: %d%n", currentRate, totalLag);
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

