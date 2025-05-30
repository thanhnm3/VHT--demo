package com.example.pipeline;

import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.MessageService;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.TopicGenerator;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AConsumer.class);
    private final String kafkaBroker;
    private final String consumerTopic;
    private final String consumerGroup;
    private final String aerospikeHost;
    private final int aerospikePort;
    private final String aerospikeNamespace;
    private final int workerPoolSize;
    private final ConfigurationService configService;
    private final KafkaConsumerService kafkaConsumerService;
    private final MessageService messageService;
    private final ExecutorService executorService;
    private final AerospikeClient aerospikeClient;
    private volatile boolean isRunning = true;

    public AConsumer(String kafkaBroker, String consumerTopic, String consumerGroup,
                    String aerospikeHost, int aerospikePort, String aerospikeNamespace,
                    int workerPoolSize) {
        this.kafkaBroker = kafkaBroker;
        this.consumerTopic = consumerTopic;
        this.consumerGroup = consumerGroup;
        this.aerospikeHost = aerospikeHost;
        this.aerospikePort = aerospikePort;
        this.aerospikeNamespace = aerospikeNamespace;
        this.workerPoolSize = workerPoolSize;
        
        // Initialize services
        this.configService = ConfigurationService.getInstance();
        this.kafkaConsumerService = new KafkaConsumerService(kafkaBroker, configService);
        
        // Initialize Aerospike client
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = 300;
        this.aerospikeClient = new AerospikeClient(clientPolicy, aerospikeHost, aerospikePort);
        
        // Initialize WritePolicy
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.totalTimeout = 5000;
        
        // Initialize MessageService with Aerospike client and policy
        this.messageService = new MessageService(aerospikeClient, writePolicy, 
                                               aerospikeNamespace, "default", consumerTopic,
                                               workerPoolSize);
        this.executorService = Executors.newFixedThreadPool(workerPoolSize);
    }

    public void start() {
        try {
            // Generate mirrored topic name
            String mirroredTopic = TopicGenerator.generateMirroredTopicName(consumerTopic);
            
            logger.info("Starting consumer with:");
            logger.info("Base topic: {}", consumerTopic);
            logger.info("Consumer topic: {}", consumerTopic);
            logger.info("Mirrored topic: {}", mirroredTopic);
            logger.info("Consumer group: {}", consumerGroup);
            logger.info("Worker pool size: {}", workerPoolSize);

            // Initialize consumers
            kafkaConsumerService.initializeConsumers(aerospikeNamespace, workerPoolSize);

            // Start consuming messages
            kafkaConsumerService.startConsuming(mirroredTopic, consumerGroup, messageService);

        } catch (Exception e) {
            logger.error("Error starting consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start consumer", e);
        }
    }

    public void shutdown() {
        isRunning = false;
        kafkaConsumerService.shutdown();
        messageService.shutdown();
        executorService.shutdown();
        if (aerospikeClient != null) {
            aerospikeClient.close();
        }
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        if (args.length < 7) {
            System.err.println("Usage: java AConsumer <kafkaBroker> <consumerTopic> <consumerGroup> " +
                             "<aerospikeHost> <aerospikePort> <aerospikeNamespace> <workerPoolSize>");
            System.exit(1);
        }

        String kafkaBroker = args[0];
        String consumerTopic = args[1];
        String consumerGroup = args[2];
        String aerospikeHost = args[3];
        int aerospikePort = Integer.parseInt(args[4]);
        String aerospikeNamespace = args[5];
        int workerPoolSize = Integer.parseInt(args[6]);

        AConsumer consumer = new AConsumer(kafkaBroker, consumerTopic, consumerGroup,
                                         aerospikeHost, aerospikePort, aerospikeNamespace,
                                         workerPoolSize);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        // Start consumer
        consumer.start();
    }
}
