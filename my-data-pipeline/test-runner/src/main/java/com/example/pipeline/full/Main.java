package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import com.example.pipeline.AConsumer;
import com.example.pipeline.AProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;

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

            // Lấy cấu hình Kafka
            String kafkaBrokerSource = config.getKafka().getBrokers().getSource();
            String kafkaBrokerTarget = config.getKafka().getBrokers().getTarget();

            // Xóa và tạo lại topic trước khi bắt đầu
            logger.info("Dang xoa tat ca topic tu 2 kafka ...");
            DeleteTopic.deleteAllTopics(kafkaBrokerSource);
            DeleteTopic.deleteAllTopics(kafkaBrokerTarget);

            // Cấu hình performance
            int producerThreadPoolSize = config.getPerformance().getWorker_pool().getProducer();
            int consumerThreadPoolSize = config.getPerformance().getWorker_pool().getConsumer();
            int maxRetries = config.getPerformance().getMax_retries();

            // Tạo thread pool cho Producer và Consumer
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> producerLatches = new ArrayList<>();
            List<CountDownLatch> consumerLatches = new ArrayList<>();

            logger.info("=== Starting Producers and Consumers ===");
            logger.info("Kafka Broker Source: {}", kafkaBrokerSource);
            logger.info("Kafka Broker Target: {}", kafkaBrokerTarget);
            logger.info("Producer Thread Pool Size: {}", producerThreadPoolSize);
            logger.info("Consumer Thread Pool Size: {}", consumerThreadPoolSize);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Khởi động các Producer và Consumer theo prefix mapping
            Map<String, List<String>> prefixMapping = config.getPrefix_mapping();
            
            // Khởi tạo producer một lần duy nhất
            Config.Producer producer = config.getProducers().get(0);
            CountDownLatch producerDone = new CountDownLatch(1);
            producerLatches.add(producerDone);

            // Tạo danh sách consumer groups cho tất cả các prefix
            String consumerGroups = String.join(",", prefixMapping.keySet().stream()
                .map(prefix -> TopicGenerator.generateAGroupName(producer.getName() + "_" + prefix))
                .toArray(String[]::new));

            // Khởi động Producer với tất cả các prefix
            executor.submit(() -> {
                try {
                    // Tạo mảng args mới với các tham số cần thiết cho Producer
                    String[] producerArgs = new String[] {
                        kafkaBrokerSource,           // kafkaBroker
                        producer.getHost(),          // aerospikeHost
                        String.valueOf(producer.getPort()), // aerospikePort
                        producer.getNamespace(),     // namespace
                        producer.getSet(),           // setName
                        String.valueOf(maxRetries),  // maxRetries
                        consumerGroups,              // consumerGroup (danh sách các group phân cách bằng dấu phẩy)
                        String.valueOf(producerThreadPoolSize), // workerPoolSize
                        String.join(",", prefixMapping.keySet().stream()
                            .map(prefix -> TopicGenerator.generateATopicName(
                                TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix)))
                            .toArray(String[]::new)) // topics (comma-separated list)
                    };
                    
                    logger.info("Starting Producer with configuration:");
                    logger.info("  Topics: {}", producerArgs[8]);
                    logger.info("  Consumer Groups: {}", consumerGroups);
                    
                    AProducer.main(producerArgs);
                } catch (Exception e) {
                    logger.error("Loi trong Producer {}: {}", producer.getName(), e.getMessage(), e);
                } finally {
                    producerDone.countDown();
                }
            });

            // Khởi động các Consumer cho từng prefix
            for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();

                // Tạo topic và consumer group cho prefix này
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix);
                String consumerTopic = TopicGenerator.generateATopicName(baseTopic);
                final String consumerGroup = TopicGenerator.generateAGroupName(producer.getName() + "_" + prefix);

                logger.info("Starting Consumers for prefix {}: {}", prefix, consumerNames);
                logger.info("  Base Topic: {}", baseTopic);
                logger.info("  Consumer Topic: {}", consumerTopic);
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
                    logger.info("  Consumer Group: {}", consumerGroup);

                    // Khởi động Consumer với consumer group tương ứng
                    executor.submit(() -> {
                        try {
                            String[] consumerArgs = new String[] {
                                kafkaBrokerTarget,           // kafkaBroker
                                consumerTopic,              // consumerTopic
                                consumerGroup,              // consumerGroup
                                consumer.getHost(),         // aerospikeHost
                                String.valueOf(consumer.getPort()), // aerospikePort
                                consumer.getNamespace(),    // aerospikeNamespace
                                String.valueOf(consumerThreadPoolSize) // workerPoolSize
                            };
                            
                            logger.info("Starting consumer with args:");
                            logger.info("  Topic: {}", consumerArgs[1]);
                            logger.info("  Consumer Group: {}", consumerArgs[2]);
                            
                            AConsumer.main(consumerArgs);
                        } catch (Exception e) {
                            logger.error("Loi trong Consumer {}: {}", consumerName, e.getMessage(), e);
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

            // Chờ tất cả producer và consumer kết thúc
            try {
                for (CountDownLatch latch : producerLatches) {
                    latch.await();
                }
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
