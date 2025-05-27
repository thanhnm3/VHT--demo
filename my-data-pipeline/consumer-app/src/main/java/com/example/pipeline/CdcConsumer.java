// package com.example.pipeline;

// import com.example.pipeline.service.config.ConsumerConfig;
// import com.example.pipeline.service.config.ConfigurationService;
// import com.example.pipeline.service.AerospikeService;
// import com.example.pipeline.service.KafkaConsumerService;
// import com.example.pipeline.service.CdcService;
// import com.google.common.util.concurrent.RateLimiter;
// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import java.util.Collections;
// import java.util.Map;
// import java.util.HashMap;
// import java.util.concurrent.CountDownLatch;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;
// import java.util.concurrent.atomic.AtomicInteger;
// import java.util.concurrent.ScheduledExecutorService;
// import java.util.concurrent.TimeUnit;

// public class CdcConsumer {
//     private static volatile boolean isShuttingDown = false;
//     private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
//     private static ConfigurationService configService;
//     private static AerospikeService aerospikeService;
//     private static KafkaConsumerService kafkaService;
//     private static Map<String, CdcService> cdcServices;
//     private static Map<String, ExecutorService> workerPools;
//     private static Map<String, RateLimiter> rateLimiters;
//     private static Map<String, AtomicInteger> messagesProcessed;

//     public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
//                           String sourceHost, int sourcePort, String sourceNamespace,
//                           String destinationHost, int destinationPort, 
//                           String kafkaBroker) {
//         try {
//             Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//                 System.out.println("Nhan tin hieu tat. Bat dau qua trinh tat an toan...");
//                 isShuttingDown = true;
//                 shutdownLatch.countDown();
//             }));

//             // Khởi tạo service cấu hình
//             configService = ConfigurationService.getInstance();
//             if (configService == null) {
//                 throw new IllegalStateException("Khong the khoi tao service cau hinh");
//             }

//             // Khởi tạo service Aerospike
//             aerospikeService = new AerospikeService(destinationHost, destinationPort);

//             // Khởi tạo service Kafka
//             kafkaService = new KafkaConsumerService(kafkaBroker, configService);
//             kafkaService.initializeConsumers(sourceNamespace, workerPoolSize);

//             // Khởi tạo các service và pool
//             cdcServices = new HashMap<>();
//             workerPools = new HashMap<>();
//             rateLimiters = new HashMap<>();
//             messagesProcessed = new HashMap<>();

//             // Khởi tạo scheduler để theo dõi số lượng message
//             ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//             scheduler.scheduleAtFixedRate(() -> {
//                 for (Map.Entry<String, AtomicInteger> entry : messagesProcessed.entrySet()) {
//                     System.out.printf("[%s] So message da xu ly: %d%n", 
//                         entry.getKey(), entry.getValue().get());
//                     entry.getValue().set(0);
//                 }
//             }, 0, 1, TimeUnit.SECONDS);

//             for (Map.Entry<String, String> entry : kafkaService.getPrefixToTopicMap().entrySet()) {
//                 String prefix = entry.getKey();
//                 String topic = entry.getValue();
                
//                 // Tạo cấu hình consumer
//                 ConsumerConfig consumerConfig = new ConsumerConfig();
//                 consumerConfig.setName("consumer-cdc-" + prefix);
//                 consumerConfig.setNamespace("consumer_cdc_" + prefix);
//                 consumerConfig.setSet("users");
                
//                 // Tạo Kafka consumer thông qua KafkaConsumerService
//                 String consumerGroup = prefix + "-cdc-group";
//                 KafkaConsumer<byte[], byte[]> consumer = kafkaService.createConsumer(topic, consumerGroup);
                
//                 // Đăng ký nhận message từ topic
//                 String mirroredTopic = "source-kafka." + topic;
//                 consumer.subscribe(Collections.singletonList(mirroredTopic));
                
//                 // Khởi tạo các service cho prefix này
//                 CdcService cdcService = new CdcService(
//                     aerospikeService.getClient(),
//                     aerospikeService.getWritePolicy(),
//                     consumerConfig.getNamespace(),
//                     consumerConfig.getSet()
//                 );
//                 cdcServices.put(prefix, cdcService);
//                 workerPools.put(prefix, Executors.newFixedThreadPool(workerPoolSize));
//                 rateLimiters.put(prefix, RateLimiter.create(maxMessagesPerSecond));
//                 messagesProcessed.put(prefix, new AtomicInteger(0));
                
//                 // Khởi chạy CDC service trong một thread riêng
//                 new Thread(() -> {
//                     try {
//                         while (!isShuttingDown) {
//                             var records = consumer.poll(java.time.Duration.ofMillis(100));
//                             for (var record : records) {
//                                 final String currentPrefix = prefix;
//                                 workerPools.get(prefix).submit(() -> {
//                                     try {
//                                         // Sử dụng RateLimiter để kiểm soát tốc độ
//                                         rateLimiters.get(currentPrefix).acquire();
//                                         cdcService.processRecord(record);
//                                         messagesProcessed.get(currentPrefix).incrementAndGet();
//                                     } catch (Exception e) {
//                                         System.err.printf("[%s] Loi xu ly record: %s%n", 
//                                             currentPrefix, e.getMessage());
//                                         e.printStackTrace();
//                                     }
//                                 });
//                             }
//                         }
//                     } catch (Exception e) {
//                         System.err.printf("[%s] Loi trong CDC service: %s%n", 
//                                         prefix, e.getMessage());
//                         e.printStackTrace();
//                     }
//                 }, prefix + "-cdc-service").start();
//             }

//             // Đợi tín hiệu tắt
//             shutdownLatch.await();
            
//             // Thực hiện tắt an toàn
//             if (isShuttingDown) {
//                 System.out.println("Bat dau qua trinh tat an toan...");
//                 // Tắt các worker pool
//                 for (ExecutorService pool : workerPools.values()) {
//                     pool.shutdown();
//                     try {
//                         if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
//                             pool.shutdownNow();
//                         }
//                     } catch (InterruptedException e) {
//                         pool.shutdownNow();
//                     }
//                 }
//                 scheduler.shutdown();
//                 kafkaService.shutdown();
//                 aerospikeService.shutdown();
//             }
            
//             System.out.println("Da tat thanh cong.");
//         } catch (Exception e) {
//             System.err.println("Loi nghiem trong: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }
// }
