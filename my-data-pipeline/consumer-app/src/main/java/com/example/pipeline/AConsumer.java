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
    
    // Static configuration parameters
    private static String kafkaBroker;
    private static String consumerTopic;
    private static String consumerGroup;
    private static String aerospikeHost;
    private static int aerospikePort;
    private static String aerospikeNamespace;
    private static String aerospikeSetName;
    private static int workerPoolSize;
    
    // Static services
    private static ConfigurationService configService;
    private static KafkaConsumerService kafkaConsumerService;
    private static MessageService messageService;
    private static ExecutorService executorService;
    private static AerospikeClient aerospikeClient;

    public static void main(String[] args) {
        try {
            if (args.length < 8) {
                System.err.println("Usage: java AConsumer <kafkaBroker> <consumerTopic> <consumerGroup> " +
                                 "<aerospikeHost> <aerospikePort> <aerospikeNamespace> <aerospikeSetName> <workerPoolSize>");
                System.exit(1);
            }

            // Initialize static configuration
            kafkaBroker = args[0];
            consumerTopic = args[1];
            consumerGroup = args[2];
            aerospikeHost = args[3];
            aerospikePort = Integer.parseInt(args[4]);
            aerospikeNamespace = args[5];
            aerospikeSetName = args[6];
            workerPoolSize = Integer.parseInt(args[7]);
            
            // Initialize services
            configService = ConfigurationService.getInstance();
            kafkaConsumerService = new KafkaConsumerService(kafkaBroker, configService);
            
            // Initialize Aerospike client
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.maxConnsPerNode = 300;
            aerospikeClient = new AerospikeClient(clientPolicy, aerospikeHost, aerospikePort);
            
            // Initialize WritePolicy
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.sendKey = true;  // Đảm bảo lưu key
            writePolicy.totalTimeout = 5000;
            
            // Initialize MessageService with Aerospike client and policy
            messageService = new MessageService(aerospikeClient, writePolicy, 
                                               aerospikeNamespace, aerospikeSetName, consumerTopic,
                                               workerPoolSize);
            executorService = Executors.newFixedThreadPool(workerPoolSize);

            // Start the consumer
            start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(AConsumer::shutdown));

        } catch (Exception e) {
            logger.error("Error in main: {}", e.getMessage(), e);
        }
    }

    private static void start() {
        try {
            // Generate mirrored topic name
            String mirroredTopic = TopicGenerator.generateMirroredTopicName(consumerTopic);
            
            logger.info("Starting consumer with:");
            logger.info("Base topic: {}", consumerTopic);
            logger.info("Consumer topic: {}", consumerTopic);
            logger.info("Mirrored topic: {}", mirroredTopic);
            logger.info("Consumer group: {}", consumerGroup);
            logger.info("Worker pool size: {}", workerPoolSize);
            logger.info("Aerospike namespace: {}", aerospikeNamespace);
            logger.info("Aerospike set name: {}", aerospikeSetName);

            // Initialize consumers
            kafkaConsumerService.initializeConsumers(aerospikeNamespace, workerPoolSize);

            // Start consuming messages from the mirrored topic
            kafkaConsumerService.startConsuming(mirroredTopic, consumerGroup, messageService);

        } catch (Exception e) {
            logger.error("Error starting consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start consumer", e);
        }
    }

    public static void shutdown() {
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
