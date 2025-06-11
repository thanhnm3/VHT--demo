package com.example.pipeline.full;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;

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
            String kafkaBrokerSource = config.getKafka().getBroker();

            // Cấu hình performance
            int producerThreadPoolSize = config.getPerformance().getWorker_pool().getProducer(); // Số thread cho Producer
            int maxRetries = config.getPerformance().getMax_retries();

            // Tạo thread pool cho Producer
            ExecutorService executor = Executors.newCachedThreadPool();
            List<CountDownLatch> producerLatches = new ArrayList<>();

            logger.info("=== Starting Producers Only ===");
            logger.info("Kafka Broker Source: {}", kafkaBrokerSource);
            logger.info("Producer Thread Pool Size: {}", producerThreadPoolSize);
            logger.info("Max Retries: {}", maxRetries);
            logger.info("===========================");

            // Khởi động một Producer duy nhất
            Config.Producer producer = config.getProducers().get(0); // Lấy producer đầu tiên
            CountDownLatch producerDone = new CountDownLatch(1);
            producerLatches.add(producerDone);

            // Tạo topic cho mỗi region
            Map<String, String> regionTopics = new HashMap<>();
            for (String region : config.getRegion_mapping().keySet()) {
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), region);
                regionTopics.put(region, baseTopic);
            }

            logger.info("Starting Producer with configuration:");
            logger.info("  Host: {}", producer.getHost());
            logger.info("  Port: {}", producer.getPort());
            logger.info("  Namespace: {}", producer.getNamespace());
            logger.info("  Set: {}", producer.getSet());
            logger.info("  Region Topics: {}", regionTopics);

            executor.submit(() -> {
                try {
                    // Tạo mảng args mới với các tham số cần thiết
                    String[] producerArgs = new String[] {
                        kafkaBrokerSource,           // kafkaBroker
                        producer.getHost(),          // aerospikeHost
                        String.valueOf(producer.getPort()), // aerospikePort
                        producer.getNamespace(),     // namespace
                        producer.getSet(),           // setName
                        String.valueOf(maxRetries),  // maxRetries
                        String.join(",", regionTopics.values()), // topics (comma-separated list)
                        String.valueOf(producerThreadPoolSize) // workerPoolSize
                    };
                    
                    AProducer.main(producerArgs);
                } catch (Exception e) {
                    logger.error("Error in Producer: {}", e.getMessage(), e);
                } finally {
                    producerDone.countDown();
                }
            });

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

            // Chờ producer kết thúc
            try {
                for (CountDownLatch latch : producerLatches) {
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