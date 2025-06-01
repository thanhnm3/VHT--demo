// package com.example.pipeline.full;

// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.TimeUnit;
// import java.util.List;
// import java.util.Map;
// import java.util.ArrayList;
// import java.util.concurrent.CountDownLatch;

// import com.example.pipeline.AProducer;
// import com.example.pipeline.service.config.Config;
// import com.example.pipeline.service.ConfigLoader;
// import com.example.pipeline.service.TopicGenerator;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// public class MainProducer {
//     private static final Logger logger = LoggerFactory.getLogger(MainProducer.class);

//     public static void main(String[] args) {
//         try {
//             // Load configuration from config.yaml
//             Config config = ConfigLoader.getConfig();
//             if (config == null) {
//                 throw new IllegalStateException("Failed to load configuration");
//             }

//             // Lấy cấu hình Kafka
//             String kafkaBrokerSource = config.getKafka().getBrokers().getSource();

//             // Cấu hình performance
//             int producerThreadPoolSize = config.getPerformance().getWorker_pool_size(); // Số thread cho Producer
//             int maxRetries = config.getPerformance().getMax_retries();

//             // Tạo thread pool cho Producer
//             ExecutorService executor = Executors.newCachedThreadPool();
//             List<CountDownLatch> producerLatches = new ArrayList<>();

//             logger.info("=== Starting Producers Only ===");
//             logger.info("Kafka Broker Source: {}", kafkaBrokerSource);
//             logger.info("Producer Thread Pool Size: {}", producerThreadPoolSize);
//             logger.info("Max Retries: {}", maxRetries);
//             logger.info("===========================");

//             // Khởi động các Producer theo prefix mapping
//             Map<String, List<String>> prefixMapping = config.getPrefix_mapping();
//             for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
//                 String prefix = entry.getKey();

//                 // Sử dụng tất cả producer cho mỗi prefix
//                 for (Config.Producer producer : config.getProducers()) {
//                     CountDownLatch producerDone = new CountDownLatch(1);
//                     producerLatches.add(producerDone);

//                     // Tạo topic theo format mới: producer1_096, producer1_033
//                     String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix);
//                     String producerTopic = TopicGenerator.generateATopicName(baseTopic);

//                     // Tạo consumer group cho A
//                     final String consumerGroup = TopicGenerator.generateAGroupName(baseTopic);

//                     logger.info("Starting Producer for prefix {}: {}", prefix, producer.getName());
//                     logger.info("  Host: {}", producer.getHost());
//                     logger.info("  Port: {}", producer.getPort());
//                     logger.info("  Namespace: {}", producer.getNamespace());
//                     logger.info("  Set: {}", producer.getSet());
//                     logger.info("  Base Topic: {}", baseTopic);
//                     logger.info("  Producer Topic: {}", producerTopic);
//                     logger.info("  Consumer Group: {}", consumerGroup);

//                     executor.submit(() -> {
//                         try {
//                             // Tạo mảng args mới với các tham số cần thiết
//                             String[] producerArgs = new String[] {
//                                 kafkaBrokerSource,           // kafkaBroker
//                                 producer.getHost(),          // aerospikeHost
//                                 String.valueOf(producer.getPort()), // aerospikePort
//                                 producer.getNamespace(),     // namespace
//                                 producer.getSet(),           // setName
//                                 String.valueOf(maxRetries),  // maxRetries
//                                 consumerGroup,              // consumerGroup
//                                 String.valueOf(producerThreadPoolSize), // workerPoolSize
//                                 producerTopic              // topics (single topic)
//                             };
                            
//                             AProducer.main(producerArgs);
//                         } catch (Exception e) {
//                             logger.error("Loi trong Producer {}: {}", producer.getName(), e.getMessage(), e);
//                         } finally {
//                             producerDone.countDown();
//                         }
//                     });
//                 }
//             }

//             // Thêm shutdown hook để xử lý khi chương trình bị tắt
//             Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                 logger.info("Dang tat chuong trinh...");
//                 executor.shutdown();
//                 try {
//                     if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
//                         executor.shutdownNow();
//                     }
//                 } catch (InterruptedException e) {
//                     executor.shutdownNow();
//                     Thread.currentThread().interrupt();
//                 }
//             }));

//             // Chờ tất cả producer kết thúc
//             try {
//                 for (CountDownLatch latch : producerLatches) {
//                     latch.await();
//                 }
//                 executor.shutdown();
//                 logger.info("Chuong trinh da ket thuc.");
//             } catch (InterruptedException e) {
//                 Thread.currentThread().interrupt();
//                 logger.error("Chuong trinh bi gian doan.");
//             }
//         } catch (Exception e) {
//             logger.error("Loi nghiem trong: {}", e.getMessage(), e);
//         }
//     }
// } 