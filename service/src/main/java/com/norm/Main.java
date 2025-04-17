package com.norm;

import io.github.cdimascio.dotenv.Dotenv;

public class Main {

    public static void main(String[] args) {
        // Load configuration from .env
        Dotenv dotenv = Dotenv.configure().directory("service//.env").load();

        // Aerospike configuration
        String aerospikeProducerHost = dotenv.get("AEROSPIKE_PRODUCER_HOST");
        int aerospikeProducerPort = Integer.parseInt(dotenv.get("AEROSPIKE_PRODUCER_PORT"));
        String producerNamespace = dotenv.get("PRODUCER_NAMESPACE");
        String producerSetName = dotenv.get("PRODUCER_SET_NAME");

        String aerospikeConsumerHost = dotenv.get("AEROSPIKE_CONSUMER_HOST");
        int aerospikeConsumerPort = Integer.parseInt(dotenv.get("AEROSPIKE_CONSUMER_PORT"));
        String consumerNamespace = dotenv.get("CONSUMER_NAMESPACE");
        String consumerSetName = dotenv.get("CONSUMER_SET_NAME");

        // Kafka configuration
        String kafkaBroker = dotenv.get("KAFKA_BROKER");
        String kafkaTopic = dotenv.get("KAFKA_TOPIC");
        String consumerGroup = dotenv.get("CONSUMER_GROUP");

        // Other configurations
        int maxMessagesPerSecond = Integer.parseInt(dotenv.get("MAX_MESSAGES_PER_SECOND"));
        int maxRetries = Integer.parseInt(dotenv.get("MAX_RETRIES"));

        // Worker pool sizes
        final int workerPoolSizeProducer = 2;
        final int workerPoolSizeConsumer = 6;

        // Start AProducer in a separate thread
        Thread producer = new Thread(() -> {
            try {
                AProducer.main(args, workerPoolSizeProducer, maxMessagesPerSecond,
                        aerospikeProducerHost, aerospikeProducerPort, producerNamespace, producerSetName,
                        kafkaBroker, kafkaTopic, maxRetries);
            } catch (Exception e) {
                System.err.println("Error in AProducer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start AConsumer in a separate thread
        Thread consumer = new Thread(() -> {
            try {
                AConsumer.main(args, workerPoolSizeConsumer, maxMessagesPerSecond,
                        aerospikeConsumerHost, aerospikeConsumerPort, consumerNamespace,
                        aerospikeProducerHost, aerospikeProducerPort, producerNamespace,
                        consumerSetName, kafkaBroker, kafkaTopic, consumerGroup);
            } catch (Exception e) {
                System.err.println("Error in AConsumer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Start both threads
        producer.start();
        consumer.start();

        // Wait for both threads to complete (if needed)
        try {
            producer.join();
            consumer.join();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
