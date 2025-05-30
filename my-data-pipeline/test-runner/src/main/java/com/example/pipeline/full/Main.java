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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

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
            logger.info("Dang xoa tat ca topic tu 2 kafka ...");
            DeleteTopic.deleteAllTopics(kafkaBrokerSource);
            DeleteTopic.deleteAllTopics(kafkaBrokerTarget);

            // Tạo thread pool cho Producer và Consumer
            ExecutorService executor = Executors.newFixedThreadPool(2);

            // Tạo latch để chờ producer và consumer kết thúc
            CountDownLatch producerDone = new CountDownLatch(1);
            CountDownLatch consumerDone = new CountDownLatch(1);

            logger.info("=== Starting Producer and Consumer ===");
            logger.info("Kafka Broker Source: {}", kafkaBrokerSource);
            logger.info("Kafka Broker Target: {}", kafkaBrokerTarget);
            logger.info("Source Host: {}", sourceHost);
            logger.info("Source Port: {}", sourcePort);
            logger.info("Source Namespace: {}", sourceNamespace);
            logger.info("Producer Set Name: {}", producerSetName);
            logger.info("Consumer Name: {}", consumerName);
            logger.info("Producer Thread Pool Size: {}", producerThreadPoolSize);
            logger.info("Consumer Thread Pool Size: {}", consumerThreadPoolSize);
            logger.info("Max Messages Per Second: {}", maxMessagesPerSecond);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Chạy Producer
            executor.submit(() -> {
                try {
                    logger.info("Khoi dong Producer...");
                    AProducer.main(args, producerThreadPoolSize, maxMessagesPerSecond,
                            sourceHost, sourcePort, sourceNamespace, producerSetName,
                            kafkaBrokerSource, maxRetries, consumerName + "-group");
                } catch (Exception e) {
                    logger.error("Loi trong Producer: {}", e.getMessage(), e);
                } finally {
                    producerDone.countDown();
                }
            });

            // Đợi một khoảng thời gian để đảm bảo topic đã được tạo và sẵn sàng
            logger.info("Dang doi 1 giay de topic duoc tao va san sang...");
            Thread.sleep(1000);

            // Chạy Consumer
            executor.submit(() -> {
                try {
                    logger.info("Khoi dong Consumer...");
                    AConsumer.main(args, consumerThreadPoolSize, maxMessagesPerSecond,
                            sourceHost, sourcePort, sourceNamespace,
                            consumer.getHost(), consumer.getPort(), 
                            kafkaBrokerTarget);
                } catch (Exception e) {
                    logger.error("Loi trong Consumer: {}", e.getMessage(), e);
                } finally {
                    consumerDone.countDown();
                }
            });

            // Them shutdown hook de xu ly khi chuong trinh bi tat
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Dang tat chuong trinh...");
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
                logger.info("Chuong trinh da ket thuc.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Chuong trinh bi gian doan.");
            }
        } catch (Exception e) {
            logger.error("Loi nghiem trong: {}", e.getMessage(), e);
        }
    }
}
