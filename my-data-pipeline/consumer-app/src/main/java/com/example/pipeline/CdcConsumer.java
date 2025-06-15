package com.example.pipeline;


import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.AerospikeService;
import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.MessageService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CdcConsumer {
    private static final Logger logger = LoggerFactory.getLogger(CdcConsumer.class);
    
    // Instance configuration parameters
    private final String kafkaBroker;
    private final String consumerTopic;
    private final String consumerGroup;
    private final String destinationHost;
    private final int destinationPort;
    private final String destinationNamespace;
    private final int workerPoolSize;
    
    // Instance services
    private final ConfigurationService configService;
    private final AerospikeService aerospikeService;
    private final KafkaConsumerService kafkaService;
    private final MessageService messageService;
    private final ExecutorService executorService;

    public CdcConsumer(String[] args) {
        if (args.length < 7) {
            throw new IllegalArgumentException("Usage: java CdcConsumer <kafkaBroker> <consumerTopic> <consumerGroup> " +
                             "<destinationHost> <destinationPort> <destinationNamespace> <workerPoolSize>");
        }

        // Initialize configuration
        this.kafkaBroker = args[0];
        this.consumerTopic = args[1];
        this.consumerGroup = args[2];
        this.destinationHost = args[3];
        this.destinationPort = Integer.parseInt(args[4]);
        this.destinationNamespace = args[5];
        this.workerPoolSize = Integer.parseInt(args[6]);
        
        // Initialize services
        this.configService = ConfigurationService.getInstance();
        if (this.configService == null) {
            throw new IllegalStateException("Cannot initialize configuration service");
        }

        this.aerospikeService = new AerospikeService(destinationHost, destinationPort);
        this.kafkaService = new KafkaConsumerService(kafkaBroker, configService);
        
        // Initialize MessageService with Aerospike client and policy
        this.messageService = new MessageService(
            aerospikeService.getClient(),
            aerospikeService.getWritePolicy(),
            destinationNamespace,
            consumerGroup.split("_")[1].split("-")[0], // Extract region from consumer group
            consumerTopic,
            workerPoolSize
        );
        
        this.executorService = Executors.newFixedThreadPool(workerPoolSize);
    }

    public static void main(String[] args) {
        try {
            CdcConsumer consumer = new CdcConsumer(args);
            consumer.start();

            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

        } catch (Exception e) {
            logger.error("Error in main: {}", e.getMessage(), e);
        }
    }

    private void start() {
        try {
            logger.info("Starting CDC consumer with:");
            logger.info("Topic: {}", consumerTopic);
            logger.info("Consumer group: {}", consumerGroup);
            logger.info("Worker pool size: {}", workerPoolSize);
            logger.info("Aerospike namespace: {}", destinationNamespace);

            // Start consuming messages from the topic using MessageService
            kafkaService.startConsuming(consumerTopic, consumerGroup, messageService);

        } catch (Exception e) {
            logger.error("Error starting CDC consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start CDC consumer", e);
        }
    }

    public void shutdown() {
        logger.info("Starting graceful shutdown...");
        
        kafkaService.shutdown();
        messageService.shutdown();
        executorService.shutdown();
        
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        aerospikeService.shutdown();
        
        logger.info("Shutdown completed successfully");
    }
}
