package com.cdc;

import io.github.cdimascio.dotenv.Dotenv;

public class Maincdc {
    public static void main(String[] args) {
        // Load configuration từ .env
        Dotenv dotenv = Dotenv.configure().directory("service//.env").load();

        // Aerospike Producer configuration
        String producerHost = dotenv.get("AEROSPIKE_PRODUCER_HOST");
        int producerPort = Integer.parseInt(dotenv.get("AEROSPIKE_PRODUCER_PORT"));
        String producerNamespace = dotenv.get("PRODUCER_NAMESPACE");
        String producerSetName = dotenv.get("PRODUCER_SET_NAME");
        String binName = "personData";
        String binLastUpdate = "last_update";

        // Aerospike Consumer configuration
        String consumerHost = dotenv.get("AEROSPIKE_CONSUMER_HOST");
        int consumerPort = Integer.parseInt(dotenv.get("AEROSPIKE_CONSUMER_PORT"));
        String consumerNamespace = dotenv.get("CONSUMER_NAMESPACE");
        String consumerSetName = dotenv.get("CONSUMER_SET_NAME");

        // Kafka configuration
        String kafkaBroker = dotenv.get("KAFKA_BROKER");
        String kafkaTopic = dotenv.get("KAFKA_TOPIC_CDC");
        String groupId = dotenv.get("CONSUMER_GROUP_CDC");
        int maxRetries = Integer.parseInt(dotenv.get("MAX_RETRIES"));

        int producerThreadPoolSize = 2; // Số lượng thread cho Producer
        int consumerThreadPoolSize = 2; // Số lượng thread cho Consumer
        int randomOperationsThreadPoolSize = 4; // Số lượng thread cho RandomOperations
        int maxMessagesPerSecond = 1000; // Giới hạn số lượng message mỗi giây
        int operationsPerSecond = 500; // Số lượng thao tác mỗi giây cho RandomOperations

        // Tạo luồng để chạy AerospikeRandomOperations
        Thread randomOperationsThread = new Thread(() -> {
            System.out.println("Starting AerospikeRandomOperations...");
            RandomOperations.main(producerHost, producerPort, producerNamespace, producerSetName, operationsPerSecond, randomOperationsThreadPoolSize);
        });

        // Tạo luồng để chạy AerospikePoller
        Thread cdcProducerThread = new Thread(() -> {
            System.out.println("Starting CdcProducer...");
            CdcProducer.start(producerHost, producerPort, producerNamespace, producerSetName, binName, binLastUpdate, kafkaBroker, kafkaTopic, maxRetries, producerThreadPoolSize);
        });

        // Tạo luồng để chạy CdcConsumer
        Thread cdcConsumerThread = new Thread(() -> {
            System.out.println("Starting CdcConsumer...");
            CdcConsumer.main(consumerHost, consumerPort, consumerNamespace, consumerSetName, kafkaBroker, kafkaTopic, groupId, consumerThreadPoolSize, maxMessagesPerSecond);
        });

        // Bắt đầu các luồng
        randomOperationsThread.start();
        cdcProducerThread.start();
        cdcConsumerThread.start();

        // Đợi cả ba luồng hoàn thành (nếu cần)
        try {
            randomOperationsThread.join();
            cdcConsumerThread.join();
            cdcProducerThread.join();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted: " + e.getMessage());
        }

        System.out.println("All applications have finished execution.");
    }
}