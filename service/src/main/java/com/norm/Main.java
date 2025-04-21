package com.norm;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        // Load configuration from .env
        Dotenv dotenv = Dotenv.configure()
                .directory("service/.env")
                .load();

        // Đọc các biến môi trường từ file .env
        String sourceHost = dotenv.get("AEROSPIKE_PRODUCER_HOST");
        int sourcePort = Integer.parseInt(dotenv.get("AEROSPIKE_PRODUCER_PORT"));
        String sourceNamespace = dotenv.get("PRODUCER_NAMESPACE");
        String destinationHost = dotenv.get("AEROSPIKE_CONSUMER_HOST");
        int destinationPort = Integer.parseInt(dotenv.get("AEROSPIKE_CONSUMER_PORT"));
        String destinationNamespace = dotenv.get("CONSUMER_NAMESPACE");
        String producerSetName = dotenv.get("PRODUCER_SET_NAME");
        String consumerSetName = dotenv.get("CONSUMER_SET_NAME");
        String kafkaBroker = dotenv.get("KAFKA_BROKER");
        String kafkaTopic = dotenv.get("KAFKA_TOPIC");
        String consumerGroup = dotenv.get("CONSUMER_GROUP");
        int producerThreadPoolSize = 2; // Số thread cho Producer
        int consumerThreadPoolSize = 6; // Số thread cho Consumer
        int maxMessagesPerSecond = Integer.parseInt(dotenv.get("MAX_MESSAGES_PER_SECOND"));
        int maxRetries = Integer.parseInt(dotenv.get("MAX_RETRIES"));

        // Xóa và tạo lại topic trước khi bắt đầu
        System.out.println("Đang xóa và tạo lại topic...");
        DeleteTopic.deleteTopic(kafkaBroker, kafkaTopic);

        // Tạo thread pool cho Producer và Consumer
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Chạy Producer trong một thread riêng
        executor.submit(() -> {
            try {
                AProducer.main(args, producerThreadPoolSize, maxMessagesPerSecond,
                        sourceHost, sourcePort, sourceNamespace, producerSetName,
                        kafkaBroker, kafkaTopic, maxRetries);
            } catch (Exception e) {
                System.err.println("Lỗi trong Producer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Chạy Consumer trong một thread riêng
        executor.submit(() -> {
            try {
                AConsumer.main(args, consumerThreadPoolSize, maxMessagesPerSecond,
                        sourceHost, sourcePort, sourceNamespace,
                        destinationHost, destinationPort, destinationNamespace,
                        consumerSetName, kafkaBroker, kafkaTopic, consumerGroup);
            } catch (Exception e) {
                System.err.println("Lỗi trong Consumer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Thêm shutdown hook để xử lý khi chương trình bị tắt
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Đang tắt chương trình...");
            executor.shutdown();
        }));
    }
}
