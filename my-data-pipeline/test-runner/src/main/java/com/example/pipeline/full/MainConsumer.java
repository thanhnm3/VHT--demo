package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import com.example.pipeline.AConsumer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainConsumer {
    private static final Logger logger = LoggerFactory.getLogger(MainConsumer.class);

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Lấy cấu hình Kafka
            String kafkaBrokerTarget = config.getKafka().getBrokers().getTarget();

            // Cấu hình performance
            int consumerThreadPoolSize = config.getPerformance().getWorker_pool().getConsumer();
            int maxRetries = config.getPerformance().getMax_retries();

            // Tạo thread pool cho Consumer
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> consumerLatches = new ArrayList<>();

            logger.info("=== Starting A Consumers Only ===");
            logger.info("Kafka Broker Target: {}", kafkaBrokerTarget);
            logger.info("Consumer Thread Pool Size: {}", consumerThreadPoolSize);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Khởi động các Consumer theo prefix mapping
            Map<String, List<String>> prefixMapping = config.getPrefix_mapping();
            for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();

                if (consumerNames.isEmpty()) {
                    logger.warn("No consumers found for prefix: {}", prefix);
                    continue;
                }

                // Tạo topic và consumer group cho prefix này
                String producerName = config.getProducers().get(0).getName();
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producerName, prefix);
                String consumerTopic = TopicGenerator.generateATopicName(baseTopic);
                String mirroredTopic = TopicGenerator.generateMirroredTopicName(consumerTopic);
                final String consumerGroup = TopicGenerator.generateAGroupName(producerName + "_" + prefix);

                logger.info("Starting Consumers for prefix {}: {}", prefix, consumerNames);
                logger.info("  Base Topic: {}", baseTopic);
                logger.info("  Consumer Topic: {}", consumerTopic);
                logger.info("  Mirrored Topic: {}", mirroredTopic);
                logger.info("  Consumer Group: {}", consumerGroup);

                // Khởi động các Consumer cho prefix này
                for (String consumerName : consumerNames) {
                    Config.Consumer consumer = config.getConsumers().stream()
                        .filter(c -> c.getName().equals(consumerName))
                        .findFirst()
                        .orElse(null);

                    if (consumer == null) {
                        logger.warn("No consumer config found for: {}", consumerName);
                        continue;
                    }

                    CountDownLatch consumerDone = new CountDownLatch(1);
                    consumerLatches.add(consumerDone);

                    logger.info("Starting A Consumer for prefix {}: {}", prefix, consumerName);
                    logger.info("  Host: {}", consumer.getHost());
                    logger.info("  Port: {}", consumer.getPort());
                    logger.info("  Namespace: {}", consumer.getNamespace());
                    logger.info("  Set: {}", consumer.getSet());
                    logger.info("  Base Topic: {}", baseTopic);
                    logger.info("  Consumer Topic: {}", consumerTopic);
                    logger.info("  Mirrored Topic: {}", mirroredTopic);
                    logger.info("  Consumer Group: {}", consumerGroup);

                    // Khởi động Consumer với mirrored topic
                    executor.submit(() -> {
                        try {
                            String[] consumerArgs = new String[] {
                                kafkaBrokerTarget,           // kafkaBroker
                                consumerTopic,              // consumerTopic
                                consumerGroup,              // consumerGroup
                                consumer.getHost(),         // aerospikeHost
                                String.valueOf(consumer.getPort()), // aerospikePort
                                consumer.getNamespace(),    // aerospikeNamespace
                                consumer.getSet(),          // aerospikeSetName
                                String.valueOf(consumerThreadPoolSize) // workerPoolSize
                            };
                            
                            AConsumer.main(consumerArgs);
                        } catch (Exception e) {
                            logger.error("Error in Consumer {}: {}", consumerName, e.getMessage(), e);
                        } finally {
                            consumerDone.countDown();
                        }
                    });
                }

            }

            // Thêm shutdown hook để xử lý khi chương trình bị tắt
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

            // Chờ tất cả consumer kết thúc
            try {
                for (CountDownLatch latch : consumerLatches) {
                    latch.await();
                }
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