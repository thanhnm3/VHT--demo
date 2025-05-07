package com.norm;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final CountDownLatch consumerReady = new CountDownLatch(1);

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
        String consumerNamespace096 = dotenv.get("CONSUMER_NAMESPACE_096");
        String consumerNamespace033 = dotenv.get("CONSUMER_NAMESPACE_033");
        String consumerSetName096 = dotenv.get("CONSUMER_SET_NAME_096");
        String consumerSetName033 = dotenv.get("CONSUMER_SET_NAME_033");
        String producerSetName = dotenv.get("PRODUCER_SET_NAME");
        String kafkaBroker = dotenv.get("KAFKA_BROKER");
        String consumerGroup096 = dotenv.get("CONSUMER_GROUP_096");
        String consumerGroup033 = dotenv.get("CONSUMER_GROUP_033");
        int producerThreadPoolSize = 2; // Số thread cho Producer
        int consumerThreadPoolSize = 4; // Số thread cho Consumer
        int maxMessagesPerSecond = Integer.parseInt(dotenv.get("MAX_MESSAGES_PER_SECOND"));
        int maxRetries = Integer.parseInt(dotenv.get("MAX_RETRIES"));

        // Xóa và tạo lại topic trước khi bắt đầu
        System.out.println("Dang xoa tat ca topic...");
        DeleteTopic.deleteAllTopics(kafkaBroker);

        // Tạo thread pool cho Producer và Consumer
        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Chạy Consumer trước
        executor.submit(() -> {
            try {
                System.out.println("Khoi dong Consumer...");
                AConsumer.main(args, consumerThreadPoolSize, maxMessagesPerSecond,
                        sourceHost, sourcePort, sourceNamespace,
                        destinationHost, destinationPort, 
                        consumerNamespace096, consumerNamespace033,
                        consumerSetName096, consumerSetName033, kafkaBroker, 
                        consumerGroup096, consumerGroup033);
                consumerReady.countDown(); // Bao hieu consumer da san sang
            } catch (Exception e) {
                System.err.println("Loi trong Consumer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Đợi consumer khởi động và ổn định
        try {
            Thread.sleep(1000);
            System.out.println("Consumer da san sang, bat dau Producer...");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Bi gian doan khi doi Consumer khoi dong");
            return;
        }

        // Chạy Producer sau khi consumer đã sẵn sàng
        executor.submit(() -> {
            try {
                AProducer.main(args, producerThreadPoolSize, maxMessagesPerSecond,
                        sourceHost, sourcePort, sourceNamespace, producerSetName,
                        kafkaBroker, maxRetries, consumerGroup096);
            } catch (Exception e) {
                System.err.println("Loi trong Producer: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Them shutdown hook de xu ly khi chuong trinh bi tat
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Dang tat chuong trinh...");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }));
    }
}
