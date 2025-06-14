package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.pipeline.AConsumer;
import com.example.pipeline.AProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainAll {
    private static final Logger logger = LoggerFactory.getLogger(MainAll.class);
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;
    private static final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

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

            // Khởi tạo producer một lần duy nhất
            Config.Producer producer = config.getProducers().get(0);
            CountDownLatch producerDone = new CountDownLatch(1);
            producerLatches.add(producerDone);

            // Tạo danh sách consumer groups cho tất cả các prefix
            String consumerGroups = String.join(",", config.getPrefix_mapping().keySet().stream()
                .map(prefix -> generateConsumerGroup(producer.getName(), prefix))
                .toArray(String[]::new));

            // Thêm shutdown hook để xử lý khi chương trình bị tắt
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("[MAIN] Shutting down pipeline...");
                isShuttingDown.set(true);
                isRunning.set(false);
                
                // Đợi producer và consumer hoàn thành
                try {
                    for (CountDownLatch latch : producerLatches) {
                        latch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                    for (CountDownLatch latch : consumerLatches) {
                        latch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // Shutdown executor
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                        logger.error("[MAIN] Shutdown timeout reached. Forcing shutdown...");
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                logger.info("[MAIN] Pipeline shutdown completed.");
            }));

            // Khởi động Producer với tất cả các prefix
            executor.submit(() -> {
                try {
                    String[] producerArgs = new String[] {
                        kafkaBrokerSource,           // kafkaBroker
                        producer.getHost(),          // aerospikeHost
                        String.valueOf(producer.getPort()), // aerospikePort
                        producer.getNamespace(),     // namespace
                        producer.getSet(),           // setName
                        String.valueOf(maxRetries),  // maxRetries
                        consumerGroups,              // consumerGroup (danh sách các group phân cách bằng dấu phẩy)
                        String.valueOf(producerThreadPoolSize), // workerPoolSize
                        String.join(",", config.getPrefix_mapping().keySet().stream()
                            .map(prefix -> TopicGenerator.generateATopicName(
                                TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix)))
                            .toArray(String[]::new)) // topics (comma-separated list)
                    };
                    
                    logger.info("[PRODUCER] Starting with configuration:");
                    logger.info("[PRODUCER] - Topics: {}", producerArgs[8]);
                    logger.info("[PRODUCER] - Consumer Groups: {}", consumerGroups);
                    logger.info("[PRODUCER] - Namespace: {}", producer.getNamespace());
                    logger.info("[PRODUCER] - Set: {}", producer.getSet());
                    
                    AProducer.main(producerArgs);
                } catch (Exception e) {
                    logger.error("[PRODUCER] Failed: {}", e.getMessage(), e);
                } finally {
                    producerDone.countDown();
                    logger.info("[PRODUCER] Completed.");
                }
            });

            // Khởi động các Consumer cho từng prefix
            for (Map.Entry<String, List<String>> entry : config.getPrefix_mapping().entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();

                // Tạo topic và consumer group cho prefix này
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix);
                String consumerTopic = TopicGenerator.generateATopicName(baseTopic);
                final String consumerGroup = generateConsumerGroup(producer.getName(), prefix);

                logger.info("[CONSUMER] Starting consumers for prefix {}: {}", prefix, consumerNames);

                // Chỉ tạo một consumer cho mỗi prefix
                String consumerName = consumerNames.get(0); // Lấy consumer đầu tiên
                Config.Consumer consumer = config.getConsumers().stream()
                    .filter(c -> c.getName().equals(consumerName))
                    .findFirst()
                    .orElse(null);

                if (consumer == null) {
                    logger.warn("[CONSUMER] Config not found for: {}", consumerName);
                    continue;
                }

                CountDownLatch consumerDone = new CountDownLatch(1);
                consumerLatches.add(consumerDone);

                // Khởi động Consumer với consumer group tương ứng
                final String finalConsumerTopic = consumerTopic;
                final String finalConsumerGroup = consumerGroup;
                executor.submit(() -> {
                    try {
                        String[] consumerArgs = new String[] {
                            kafkaBrokerTarget,           // kafkaBroker
                            finalConsumerTopic,         // consumerTopic
                            finalConsumerGroup,         // consumerGroup
                            consumer.getHost(),         // aerospikeHost
                            String.valueOf(consumer.getPort()), // aerospikePort
                            consumer.getNamespace(),    // aerospikeNamespace
                            consumer.getSet(),          // aerospikeSetName
                            String.valueOf(consumerThreadPoolSize) // workerPoolSize
                        };
                        
                        logger.info("[CONSUMER] Starting {}:", consumerName);
                        logger.info("[CONSUMER] - Topic: {}", finalConsumerTopic);
                        logger.info("[CONSUMER] - Group: {}", finalConsumerGroup);
                        logger.info("[CONSUMER] - Namespace: {}", consumer.getNamespace());
                        logger.info("[CONSUMER] - Set: {}", consumer.getSet());
                        
                        AConsumer.main(consumerArgs);
                    } catch (Exception e) {
                        logger.error("[CONSUMER] {} failed: {}", consumerName, e.getMessage(), e);
                    } finally {
                        consumerDone.countDown();
                        logger.info("[CONSUMER] {} completed.", consumerName);
                    }
                });
            }

            // Chờ producer và consumer hoàn thành
            try {
                // Chờ producer hoàn thành
                for (CountDownLatch latch : producerLatches) {
                    latch.await();
                }
                logger.info("[MAIN] Producer completed.");
                
                // Chờ consumer hoàn thành
                for (CountDownLatch latch : consumerLatches) {
                    latch.await();
                }
                logger.info("[MAIN] All consumers completed.");
                
                // Shutdown executor và chờ tất cả tasks hoàn thành
                executor.shutdown();
                if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    logger.error("[MAIN] Executor shutdown timeout reached. Forcing shutdown...");
                    executor.shutdownNow();
                }
                
                logger.info("[MAIN] Pipeline completed successfully.");
                
                // Tự động thoát sau khi hoàn thành
                logger.info("[MAIN] Auto-shutting down after completion...");
                System.exit(0);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("[MAIN] Pipeline interrupted.");
                System.exit(1);
            }
        } catch (Exception e) {
            logger.error("Loi nghiem trong: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    // Phương thức chung để tạo consumer group
    private static String generateConsumerGroup(String producerName, String prefix) {
        return TopicGenerator.generateAGroupName(producerName + "_" + prefix);
    }
}
