package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;

import com.example.pipeline.AConsumer;
import com.example.pipeline.AProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.concurrent.CountDownLatch;

public class Main {
    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Lấy cấu hình Producer
            String sourceHost = config.getProducers().get(0).getHost();
            int sourcePort = config.getProducers().get(0).getPort();
            String sourceNamespace = config.getProducers().get(0).getNamespace();
            String producerSetName = config.getProducers().get(0).getSet();
            String kafkaBrokerSource = config.getKafka().getBrokers().getSource();
            String kafkaBrokerTarget = config.getKafka().getBrokers().getTarget();
            
            // Lấy cấu hình Consumer
            Map<String, List<String>> prefixMapping = config.getPrefix_mapping();
            String consumerName = prefixMapping.values().iterator().next().get(0);
            Config.Consumer consumer = config.getConsumers().stream()
                .filter(c -> c.getName().equals(consumerName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No consumer config found"));

            // Cấu hình performance
            int producerThreadPoolSize = 2; // Số thread cho Producer
            int consumerThreadPoolSize = 4; // Số thread cho Consumer
            int maxMessagesPerSecond = config.getPerformance().getMax_messages_per_second();
            int maxRetries = config.getPerformance().getMax_retries();

            // Xóa và tạo lại topic trước khi bắt đầu
            System.out.println("Dang xoa tat ca topic tu 2 kafka ...");
            DeleteTopic.deleteAllTopics(kafkaBrokerSource);
            DeleteTopic.deleteAllTopics(kafkaBrokerTarget);

            // Tạo thread pool cho Producer và Consumer
            ExecutorService executor = Executors.newFixedThreadPool(2);

            // Tạo latch để chờ producer và consumer kết thúc
            CountDownLatch producerDone = new CountDownLatch(1);
            CountDownLatch consumerDone = new CountDownLatch(1);

            System.out.println("=== Starting Producer and Consumer ===");
            System.out.println("Kafka Broker Source: " + kafkaBrokerSource);
            System.out.println("Kafka Broker Target: " + kafkaBrokerTarget);
            System.out.println("Source Host: " + sourceHost);
            System.out.println("Source Port: " + sourcePort);
            System.out.println("Source Namespace: " + sourceNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("Consumer Name: " + consumerName);
            System.out.println("Producer Thread Pool Size: " + producerThreadPoolSize);
            System.out.println("Consumer Thread Pool Size: " + consumerThreadPoolSize);
            System.out.println("Max Messages Per Second: " + maxMessagesPerSecond);
            System.out.println("Max Retries: " + maxRetries);
            System.out.println("===========================");

            // Chạy Producer
            executor.submit(() -> {
                try {
                    System.out.println("Khoi dong Producer...");
                    AProducer.main(args, producerThreadPoolSize, maxMessagesPerSecond,
                            sourceHost, sourcePort, sourceNamespace, producerSetName,
                            kafkaBrokerSource, maxRetries, consumerName + "-group");
                } catch (Exception e) {
                    System.err.println("Loi trong Producer: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    producerDone.countDown();
                }
            });

            // Chạy Consumer
            executor.submit(() -> {
                try {
                    System.out.println("Khoi dong Consumer...");
                    AConsumer.main(args, consumerThreadPoolSize, maxMessagesPerSecond,
                            sourceHost, sourcePort, sourceNamespace,
                            consumer.getHost(), consumer.getPort(), 
                            kafkaBrokerTarget);
                } catch (Exception e) {
                    System.err.println("Loi trong Consumer: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    consumerDone.countDown();
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

            // Chờ cả producer và consumer kết thúc
            try {
                producerDone.await();
                consumerDone.await();
                executor.shutdown();
                System.out.println("Chuong trinh da ket thuc.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Chuong trinh bi gian doan.");
            }
        } catch (Exception e) {
            System.err.println("Loi nghiem trong: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
