package com.example.pipeline.cdc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;


import com.example.pipeline.CdcConsumer;
import com.example.pipeline.CdcProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.full.DeleteTopic;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Maincdc {
    private static final Logger logger = LoggerFactory.getLogger(Maincdc.class);

    public static void main(String[] args) {
        try {
            runFullPipeline();
        } catch (Exception e) {
            logger.error("Critical error: {}", e.getMessage(), e);
        }
    }

    public static void runFullPipeline() {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Lấy cấu hình Kafka
            String kafkaBroker = config.getKafka().getBroker();

            // Xóa và tạo lại topic trước khi bắt đầu
            logger.info("Deleting all topics from Kafka broker: {}", kafkaBroker);
            DeleteTopic.deleteAllTopics(kafkaBroker);
            logger.info("All topics have been deleted successfully");

            // Cấu hình performance
            int producerThreadPoolSize = config.getPerformance().getWorker_pool().getProducer();
            int consumerThreadPoolSize = config.getPerformance().getWorker_pool().getConsumer();
            int maxMessagesPerSecond = config.getPerformance().getMax_messages_per_second();
            int maxRetries = config.getPerformance().getMax_retries();

            // Tạo thread pool cho Producer, Consumer và Random Operations
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> producerLatches = new ArrayList<>();
            List<CountDownLatch> consumerLatches = new ArrayList<>();
            List<CountDownLatch> randomOpLatches = new ArrayList<>();

            // Khởi tạo producer một lần duy nhất
            Config.Producer producer = config.getProducers().get(0);
            CountDownLatch producerDone = new CountDownLatch(1);
            producerLatches.add(producerDone);

            // Tạo danh sách topic và consumer groups cho CDC
            StringBuilder topicList = new StringBuilder();
            StringBuilder consumerGroupList = new StringBuilder();
            
            for (Map.Entry<String, List<String>> entry : config.getRegion_mapping().entrySet()) {
                String region = entry.getKey();
                String cdcTopic = producer.getName() + "_" + region + "-cdc";
                String cdcGroup = cdcTopic + "-group";
                
                if (topicList.length() > 0) {
                    topicList.append(",");
                    consumerGroupList.append(",");
                }
                topicList.append(cdcTopic);
                consumerGroupList.append(cdcGroup);
            }

            // Khởi động Producer với tất cả các region
            executor.submit(() -> {
                try {
                    String[] producerArgs = new String[] {
                        kafkaBroker,                // kafkaBroker
                        producer.getHost(),         // aerospikeHost
                        String.valueOf(producer.getPort()), // aerospikePort
                        producer.getNamespace(),    // namespace
                        producer.getSet(),          // setName
                        String.valueOf(maxRetries), // maxRetries
                        consumerGroupList.toString(), // consumerGroup
                        String.valueOf(producerThreadPoolSize), // workerPoolSize
                        String.valueOf(maxMessagesPerSecond) // maxMessagesPerSecond for rate control
                    };
                    
                    logger.info("[CDC PRODUCER] Starting with configuration:");
                    logger.info("[CDC PRODUCER] - Topics: {}", topicList);
                    logger.info("[CDC PRODUCER] - Consumer Groups: {}", consumerGroupList);
                    logger.info("[CDC PRODUCER] - Namespace: {}", producer.getNamespace());
                    logger.info("[CDC PRODUCER] - Set: {}", producer.getSet());
                    logger.info("[CDC PRODUCER] - Max Messages Per Second: {}", maxMessagesPerSecond);
                    
                    CdcProducer.main(producerArgs);
                } catch (Exception e) {
                    logger.error("[CDC PRODUCER] Failed: {}", e.getMessage(), e);
                } finally {
                    producerDone.countDown();
                }
            });

            // Đợi một khoảng thời gian để đảm bảo topic đã được tạo và sẵn sàng
            logger.info("Waiting for topics to be created and ready...");
            Thread.sleep(1000);

            // Khởi động Consumer cho CDC
            for (Map.Entry<String, List<String>> entry : config.getRegion_mapping().entrySet()) {
                String region = entry.getKey();
                List<String> consumerNames = entry.getValue();
                String consumerName = consumerNames.get(0);

                Config.Consumer consumer = config.getConsumers().stream()
                    .filter(c -> c.getName().equals(consumerName))
                    .findFirst()
                    .orElse(null);

                if (consumer == null) {
                    logger.warn("[CDC CONSUMER] Config not found for: {}", consumerName);
                    continue;
                }

                CountDownLatch consumerDone = new CountDownLatch(1);
                consumerLatches.add(consumerDone);

                // Tạo topic và consumer group cho region này
                String cdcTopic = producer.getName() + "_" + region + "-cdc";
                String cdcGroup = cdcTopic + "-group";

                executor.submit(() -> {
                    try {
                        String[] consumerArgs = new String[] {
                            kafkaBroker,                // kafkaBroker
                            cdcTopic,                  // consumerTopic
                            cdcGroup,                  // consumerGroup
                            consumer.getHost(),         // destinationHost
                            String.valueOf(consumer.getPort()), // destinationPort
                            consumer.getNamespace(),    // destinationNamespace
                            String.valueOf(consumerThreadPoolSize) // workerPoolSize
                        };
                        
                        logger.info("[CDC CONSUMER] Starting {} for region {}:", consumerName, region);
                        logger.info("[CDC CONSUMER] - Topic: {}", cdcTopic);
                        logger.info("[CDC CONSUMER] - Group: {}", cdcGroup);
                        logger.info("[CDC CONSUMER] - Destination Namespace: {}", consumer.getNamespace());
                        logger.info("[CDC CONSUMER] - Set: {}", consumer.getSet());
                        
                        CdcConsumer.main(consumerArgs);
                    } catch (Exception e) {
                        logger.error("[CDC CONSUMER] {} failed for region {}: {}", consumerName, region, e.getMessage(), e);
                    } finally {
                        consumerDone.countDown();
                    }
                });

                // Khởi động Random Operations cho mỗi region
                CountDownLatch randomOpDone = new CountDownLatch(1);
                randomOpLatches.add(randomOpDone);

                executor.submit(() -> {
                    try {
                        logger.info("[RANDOM OPERATIONS] Starting for region {}:", region);
                        logger.info("[RANDOM OPERATIONS] - Source Namespace: {}", producer.getNamespace());
                        logger.info("[RANDOM OPERATIONS] - Source Set: {}", producer.getSet());
                        
                        RandomOperations.main(
                            producer.getHost(),
                            producer.getPort(),
                            producer.getNamespace(),
                            producer.getSet(),
                            maxMessagesPerSecond,
                            producerThreadPoolSize
                        );
                    } catch (Exception e) {
                        logger.error("[RANDOM OPERATIONS] Failed for region {}: {}", region, e.getMessage(), e);
                    } finally {
                        randomOpDone.countDown();
                    }
                });
            }

            // Thêm shutdown hook để xử lý khi chương trình bị tắt
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("[MAIN] Shutting down CDC pipeline...");
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

            // Chờ tất cả producer, consumer và random operations kết thúc
            try {
                for (CountDownLatch latch : producerLatches) {
                    latch.await();
                }
                for (CountDownLatch latch : consumerLatches) {
                    latch.await();
                }
                for (CountDownLatch latch : randomOpLatches) {
                    latch.await();
                }
                executor.shutdown();
                logger.info("[MAIN] CDC Pipeline completed successfully.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("[MAIN] CDC Pipeline interrupted.");
            }
        } catch (Exception e) {
            logger.error("Critical error: {}", e.getMessage(), e);
        }
    }
}