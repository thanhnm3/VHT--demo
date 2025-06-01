package com.example.pipeline;

import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.MessageService;
import com.example.pipeline.service.config.ConfigurationService;
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
    
    // Instance configuration parameters
    private final String kafkaBroker;
    private final String consumerTopic;
    private final String consumerGroup;
    private final String aerospikeHost;
    private final int aerospikePort;
    private final String aerospikeNamespace;
    private final String aerospikeSetName;
    private final int workerPoolSize;
    
    // Instance services
    private final ConfigurationService configService;
    private final KafkaConsumerService kafkaConsumerService;
    private final MessageService messageService;
    private final ExecutorService executorService;
    private final AerospikeClient aerospikeClient;

    public AConsumer(String[] args) {
        if (args.length < 8) {
            throw new IllegalArgumentException("Usage: java AConsumer <kafkaBroker> <consumerTopic> <consumerGroup> " +
                             "<aerospikeHost> <aerospikePort> <aerospikeNamespace> <aerospikeSetName> <workerPoolSize>");
        }

        // Initialize configuration
        this.kafkaBroker = args[0];
        this.consumerTopic = args[1];
        this.consumerGroup = args[2];
        this.aerospikeHost = args[3];
        this.aerospikePort = Integer.parseInt(args[4]);
        this.aerospikeNamespace = args[5];
        this.aerospikeSetName = args[6];
        this.workerPoolSize = Integer.parseInt(args[7]);
        
        // Initialize services
        this.configService = ConfigurationService.getInstance();
        this.kafkaConsumerService = new KafkaConsumerService(kafkaBroker, configService);
        
        // Initialize Aerospike client
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.maxConnsPerNode = 300;
        this.aerospikeClient = new AerospikeClient(clientPolicy, aerospikeHost, aerospikePort);
        
        // Initialize WritePolicy
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;  // Đảm bảo lưu key
        writePolicy.totalTimeout = 5000;
        
        // Initialize MessageService with Aerospike client and policy
        this.messageService = new MessageService(aerospikeClient, writePolicy, 
                                               aerospikeNamespace, aerospikeSetName, consumerTopic,
                                               workerPoolSize);
        this.executorService = Executors.newFixedThreadPool(workerPoolSize);
    }

    public static void main(String[] args) {
        try {
            AConsumer consumer = new AConsumer(args);
            consumer.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        } catch (Exception e) {
            logger.error("Error in main: {}", e.getMessage(), e);
        }
    }

    private void start() {
        try {
            // Generate mirrored topic name
            String mirroredTopic = TopicGenerator.generateMirroredTopicName(consumerTopic);
            
            logger.info("Starting consumer with:");
            logger.info("Topic: {}", consumerTopic);
            logger.info("Mirrored topic: {}", mirroredTopic);
            logger.info("Consumer group: {}", consumerGroup);
            logger.info("Worker pool size: {}", workerPoolSize);
            logger.info("Aerospike namespace: {}", aerospikeNamespace);
            logger.info("Aerospike set name: {}", aerospikeSetName);

            // Start consuming messages from the mirrored topic
            kafkaConsumerService.startConsuming(mirroredTopic, consumerGroup, messageService);

        } catch (Exception e) {
            logger.error("Error starting consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start consumer", e);
        }
    }

    public void shutdown() {
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
}
