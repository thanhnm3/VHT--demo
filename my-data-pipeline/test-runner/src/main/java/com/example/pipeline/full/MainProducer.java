package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

import com.example.pipeline.AProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainProducer {
    private static final Logger logger = LoggerFactory.getLogger(MainProducer.class);

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Lấy cấu hình Kafka
            String kafkaBroker = config.getKafka().getBroker();

            // Xóa và tạo lại topic trước khi bắt đầu
            logger.info("Dang xoa tat ca topic tu kafka ...");
            DeleteTopic.deleteAllTopics(kafkaBroker);
            Thread.sleep(10000);

            // Cấu hình performance
            int producerThreadPoolSize = config.getPerformance().getWorker_pool().getProducer();
            int maxRetries = config.getPerformance().getMax_retries();

            // Tạo thread pool cho Producer
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> producerLatches = new ArrayList<>();

            logger.info("=== Starting Producers Only ===");
            logger.info("Kafka Broker: {}", kafkaBroker);
            logger.info("Producer Thread Pool Size: {}", producerThreadPoolSize);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Khởi tạo producer một lần duy nhất
            Config.Producer producer = config.getProducers().get(0);
            CountDownLatch producerDone = new CountDownLatch(1);
            producerLatches.add(producerDone);

            // Tạo danh sách consumer groups cho tất cả các prefix
            String consumerGroups = String.join(",", config.getPrefix_mapping().keySet().stream()
                .map(prefix -> generateConsumerGroup(producer.getName(), prefix))
                .toArray(String[]::new));

            // Khởi động Producer với tất cả các prefix
            executor.submit(() -> {
                try {
                    String[] producerArgs = new String[] {
                        kafkaBroker,           // kafkaBroker
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
                }
            });

            // Thêm shutdown hook để xử lý khi chương trình bị tắt
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("[MAIN] Shutting down producers...");
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

            // Chờ tất cả producer kết thúc
            try {
                for (CountDownLatch latch : producerLatches) {
                    latch.await();
                }
                executor.shutdown();
                logger.info("[MAIN] All producers completed successfully.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("[MAIN] Pipeline interrupted.");
            }
        } catch (Exception e) {
            logger.error("[MAIN] Critical error: {}", e.getMessage(), e);
        }
    }

    // Phương thức chung để tạo consumer group
    private static String generateConsumerGroup(String producerName, String prefix) {
        return TopicGenerator.generateAGroupName(producerName + "_" + prefix);
    }
} 