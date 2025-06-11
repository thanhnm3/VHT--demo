// package com.example.pipeline.cdc;

// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.TimeUnit;
// import java.util.List;
// import java.util.Map;
// import java.util.ArrayList;
// import java.util.concurrent.CountDownLatch;

// import com.example.pipeline.CdcConsumer;
// import com.example.pipeline.CdcProducer;
// import com.example.pipeline.service.config.Config;
// import com.example.pipeline.service.ConfigLoader;
// import com.example.pipeline.full.DeleteTopic;
// import com.example.pipeline.service.TopicGenerator;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// public class Maincdc {
//     private static final Logger logger = LoggerFactory.getLogger(Maincdc.class);

//     public static void main(String[] args) {
//         try {
//             // Load configuration from config.yaml
//             Config config = ConfigLoader.getConfig();
//             if (config == null) {
//                 throw new IllegalStateException("Failed to load configuration");
//             }

//             // Lấy cấu hình Kafka
//             String kafkaBrokerSource = config.getKafka().getBrokers().getSource();
//             String kafkaBrokerTarget = config.getKafka().getBrokers().getTarget();

//             // Xóa và tạo lại topic trước khi bắt đầu
//             logger.info("Deleting all topics from source Kafka broker: {}", kafkaBrokerSource);
//             DeleteTopic.deleteAllTopics(kafkaBrokerSource);
//             logger.info("Deleting all topics from target Kafka broker: {}", kafkaBrokerTarget);
//             DeleteTopic.deleteAllTopics(kafkaBrokerTarget);
//             logger.info("All topics have been deleted successfully");

//             // Cấu hình performance
//             int producerThreadPoolSize = config.getPerformance().getWorker_pool().getProducer();
//             int consumerThreadPoolSize = config.getPerformance().getWorker_pool().getConsumer();
//             int randomOperationsThreadPoolSize = 4; // Số thread cho RandomOperations
//             int maxMessagesPerSecond = config.getPerformance().getMax_messages_per_second();
//             int operationsPerSecond = 500; // Số lượng thao tác mỗi giây cho RandomOperations
//             int maxRetries = config.getPerformance().getMax_retries();

//             // Tạo thread pool cho Producer, Consumer và RandomOperations
//             ExecutorService executor = Executors.newCachedThreadPool();
//             List<CountDownLatch> producerLatches = new ArrayList<>();
//             List<CountDownLatch> consumerLatches = new ArrayList<>();
//             CountDownLatch randomOpsDone = new CountDownLatch(1);

//             // Khởi tạo producer một lần duy nhất
//             Config.Producer producer = config.getProducers().get(0);
//             CountDownLatch producerDone = new CountDownLatch(1);
//             producerLatches.add(producerDone);

//             // Tạo danh sách topic và consumer groups cho CDC
//             StringBuilder topicList = new StringBuilder();
//             StringBuilder consumerGroupList = new StringBuilder();
            
//             for (Map.Entry<String, List<String>> entry : config.getPrefix_mapping().entrySet()) {
//                 String prefix = entry.getKey();
//                 String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix);
//                 String cdcTopic = TopicGenerator.generateCdcTopicName(baseTopic);
                
//                 if (topicList.length() > 0) {
//                     topicList.append(",");
//                     consumerGroupList.append(",");
//                 }
//                 topicList.append(cdcTopic);
//                 consumerGroupList.append(TopicGenerator.generateCdcGroupName(cdcTopic));
//             }

//             // Khởi động RandomOperations
//             executor.submit(() -> {
//                 try {
//                     logger.info("[RANDOM OPS] Starting with configuration:");
//                     logger.info("[RANDOM OPS] - Host: {}", producer.getHost());
//                     logger.info("[RANDOM OPS] - Port: {}", producer.getPort());
//                     logger.info("[RANDOM OPS] - Namespace: {}", producer.getNamespace());
//                     logger.info("[RANDOM OPS] - Set: {}", producer.getSet());
                    
//                     RandomOperations.main(
//                         producer.getHost(),
//                         producer.getPort(),
//                         producer.getNamespace(),
//                         producer.getSet(),
//                         operationsPerSecond,
//                         randomOperationsThreadPoolSize
//                     );
//                 } catch (Exception e) {
//                     logger.error("[RANDOM OPS] Failed: {}", e.getMessage(), e);
//                 } finally {
//                     randomOpsDone.countDown();
//                 }
//             });

//             // Khởi động Producer với tất cả các prefix
//             executor.submit(() -> {
//                 try {
//                     String[] producerArgs = new String[] {
//                         kafkaBrokerSource,           // kafkaBroker
//                         producer.getHost(),          // aerospikeHost
//                         String.valueOf(producer.getPort()), // aerospikePort
//                         producer.getNamespace(),     // namespace
//                         producer.getSet(),           // setName
//                         String.valueOf(maxRetries),  // maxRetries
//                         consumerGroupList.toString(), // consumerGroup
//                         String.valueOf(producerThreadPoolSize), // workerPoolSize
//                         topicList.toString(),        // topics
//                         String.valueOf(maxMessagesPerSecond) // maxMessagesPerSecond for rate control
//                     };
                    
//                     logger.info("[CDC PRODUCER] Starting with configuration:");
//                     logger.info("[CDC PRODUCER] - Topics: {}", topicList);
//                     logger.info("[CDC PRODUCER] - Consumer Groups: {}", consumerGroupList);
//                     logger.info("[CDC PRODUCER] - Namespace: {}", producer.getNamespace());
//                     logger.info("[CDC PRODUCER] - Set: {}", producer.getSet());
//                     logger.info("[CDC PRODUCER] - Max Messages Per Second: {}", maxMessagesPerSecond);
                    
//                     CdcProducer.main(producerArgs);
//                 } catch (Exception e) {
//                     logger.error("[CDC PRODUCER] Failed: {}", e.getMessage(), e);
//                 } finally {
//                     producerDone.countDown();
//                 }
//             });

//             // Đợi một khoảng thời gian để đảm bảo topic đã được tạo và sẵn sàng
//             logger.info("Waiting for topics to be created and ready...");
//             Thread.sleep(1000);

//             // Khởi động Consumer cho CDC
//             for (Map.Entry<String, List<String>> entry : config.getPrefix_mapping().entrySet()) {
//                 String prefix = entry.getKey();
//                 List<String> consumerNames = entry.getValue();
//                 String consumerName = consumerNames.get(0);

//                 Config.Consumer consumer = config.getConsumers().stream()
//                     .filter(c -> c.getName().equals(consumerName))
//                     .findFirst()
//                     .orElse(null);

//                 if (consumer == null) {
//                     logger.warn("[CDC CONSUMER] Config not found for: {}", consumerName);
//                     continue;
//                 }

//                 CountDownLatch consumerDone = new CountDownLatch(1);
//                 consumerLatches.add(consumerDone);

//                 // Tạo topic và consumer group cho prefix này
//                 String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix);
//                 String cdcTopic = TopicGenerator.generateCdcTopicName(baseTopic);
//                 String cdcGroup = TopicGenerator.generateCdcGroupName(cdcTopic);

//                 // Tạo mirrored topic name cho consumer
//                 String mirroredTopic = "source-kafka." + cdcTopic;

//                 executor.submit(() -> {
//                     try {
//                         String[] consumerArgs = new String[] {
//                             kafkaBrokerTarget,           // kafkaBroker
//                             mirroredTopic,              // consumerTopic (mirrored topic)
//                             cdcGroup,                    // consumerGroup
//                             consumer.getHost(),          // destinationHost
//                             String.valueOf(consumer.getPort()), // destinationPort
//                             consumer.getNamespace(),     // destinationNamespace
//                             String.valueOf(consumerThreadPoolSize) // workerPoolSize
//                         };
                        
//                         logger.info("[CDC CONSUMER] Starting {} for prefix {}:", consumerName, prefix);
//                         logger.info("[CDC CONSUMER] - Topic: {}", mirroredTopic);
//                         logger.info("[CDC CONSUMER] - Group: {}", cdcGroup);
//                         logger.info("[CDC CONSUMER] - Destination Namespace: {}", consumer.getNamespace());
//                         logger.info("[CDC CONSUMER] - Set: {}", consumer.getSet());
                        
//                         CdcConsumer.main(consumerArgs);
//                     } catch (Exception e) {
//                         logger.error("[CDC CONSUMER] {} failed for prefix {}: {}", consumerName, prefix, e.getMessage(), e);
//                     } finally {
//                         consumerDone.countDown();
//                     }
//                 });
//             }

//             // Thêm shutdown hook để xử lý khi chương trình bị tắt
//             Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                 logger.info("[MAIN] Shutting down CDC pipeline...");
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

//             // Chờ tất cả producer, consumer và random operations kết thúc
//             try {
//                 randomOpsDone.await();
//                 for (CountDownLatch latch : producerLatches) {
//                     latch.await();
//                 }
//                 for (CountDownLatch latch : consumerLatches) {
//                     latch.await();
//                 }
//                 executor.shutdown();
//                 logger.info("[MAIN] CDC Pipeline completed successfully.");
//             } catch (InterruptedException e) {
//                 Thread.currentThread().interrupt();
//                 logger.error("[MAIN] CDC Pipeline interrupted.");
//             }
//         } catch (Exception e) {
//             logger.error("Critical error: {}", e.getMessage(), e);
//         }
//     }
// }