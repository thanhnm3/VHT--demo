package com.example.pipeline;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.AerospikeService;
import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.CdcService;
import com.example.pipeline.service.TopicGenerator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.List;

public class CdcConsumer {
    private static volatile boolean isShuttingDown = false;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private static ConfigurationService configService;
    private static AerospikeService aerospikeService;
    private static KafkaConsumerService kafkaService;
    private static Map<String, CdcService> cdcServices;
    private static Map<String, ExecutorService> workerPools;
    private static Map<String, AtomicInteger> messagesProcessed;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String sourceHost, int sourcePort, String sourceNamespace,
                          String destinationHost, int destinationPort, 
                          String kafkaBroker, String consumerGroup) {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Nhan tin hieu tat. Bat dau qua trinh tat an toan...");
                isShuttingDown = true;
                shutdownLatch.countDown();
            }));

            // Khởi tạo service cấu hình
            configService = ConfigurationService.getInstance();
            if (configService == null) {
                throw new IllegalStateException("Khong the khoi tao service cau hinh");
            }

            // Khởi tạo service Aerospike
            aerospikeService = new AerospikeService(destinationHost, destinationPort);

            // Khởi tạo service Kafka
            kafkaService = new KafkaConsumerService(kafkaBroker, configService);
            kafkaService.initializeConsumers(sourceNamespace, workerPoolSize);

            // Khởi tạo các service và pool
            cdcServices = new HashMap<>();
            workerPools = new HashMap<>();
            messagesProcessed = new HashMap<>();

            // Hiển thị thông tin cấu hình tĩnh
            System.out.println("\n=== CDC Consumer Configuration ===");
            System.out.println("Kafka Broker: " + kafkaBroker);
            System.out.println("Source Host: " + sourceHost);
            System.out.println("Source Port: " + sourcePort);
            System.out.println("Source Namespace: " + sourceNamespace);
            System.out.println("Destination Host: " + destinationHost);
            System.out.println("Destination Port: " + destinationPort);
            System.out.println("Worker Pool Size: " + workerPoolSize);
            System.out.println("Max Messages Per Second: " + maxMessagesPerSecond);
            System.out.println("Consumer Group: " + consumerGroup);
            System.out.println("\nTopic Mapping:");
            for (Map.Entry<String, String> entry : kafkaService.getPrefixToTopicMap().entrySet()) {
                String prefix = entry.getKey();
                String topic = entry.getValue();
                String cdcTopic = TopicGenerator.generateCdcTopicName(topic);
                System.out.printf("  Prefix: %s\n", prefix);
                System.out.printf("    Original Topic: %s\n", cdcTopic);
                System.out.printf("    Target Topic: source-kafka.%s\n", cdcTopic);
            }
            System.out.println("===============================\n");

            // Khởi tạo scheduler để theo dõi số lượng message
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(() -> {
                StringBuilder stats = new StringBuilder("\n=== Consumer Stats ===\n");
                for (Map.Entry<String, AtomicInteger> entry : messagesProcessed.entrySet()) {
                    if (entry.getValue().get() > 0) {  // Chỉ log khi có message được xử lý
                        stats.append(String.format("  Prefix %s: %d messages/s\n", 
                            entry.getKey(), entry.getValue().get()));
                        entry.getValue().set(0);
                    }
                }
                stats.append("====================\n");
                System.out.print(stats);
            }, 0, 5, TimeUnit.SECONDS);

            Map<String, List<String>> prefixMapping = configService.getPrefixMappings();
            Map<String, String> prefixToTopicMap = kafkaService.getPrefixToTopicMap();
            
            for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();
                
                if (consumerNames.isEmpty()) {
                    System.err.println("Warning: No consumers found for prefix " + prefix);
                    continue;
                }

                // Get the first consumer for this prefix
                String consumerName = consumerNames.get(0);
                Config.Consumer consumer = configService.getConsumerConfig(consumerName);
                if (consumer == null) {
                    System.err.println("Warning: No consumer config found for " + consumerName);
                    continue;
                }

                String topic = prefixToTopicMap.get(prefix);
                if (topic == null) {
                    System.err.println("Warning: No topic found for prefix " + prefix);
                    continue;
                }
                
                // Tạo Kafka consumer thông qua KafkaConsumerService
                String cdcTopic = TopicGenerator.generateCdcTopicName(topic);
                System.out.printf("[CDC Consumer] Starting consumer for prefix %s\n", prefix);
                System.out.printf("[CDC Consumer] Subscribing to topic: source-kafka.%s\n", cdcTopic);
                
                KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaService.createConsumer("source-kafka." + cdcTopic, consumerGroup);
                
                // Khởi tạo các service cho prefix này
                CdcService cdcService = new CdcService(
                    aerospikeService.getClient(),
                    aerospikeService.getWritePolicy(),
                    consumer.getNamespace(),
                    consumer.getSet()
                );
                cdcServices.put(prefix, cdcService);
                workerPools.put(prefix, Executors.newFixedThreadPool(workerPoolSize));
                messagesProcessed.put(prefix, new AtomicInteger(0));
                
                // Khởi chạy CDC service trong một thread riêng
                final String currentPrefix = prefix;
                new Thread(() -> {
                    try {
                        while (!isShuttingDown) {
                            var records = kafkaConsumer.poll(java.time.Duration.ofMillis(100));
                            for (var record : records) {
                                workerPools.get(currentPrefix).submit(() -> {
                                    try {
                                        cdcService.processRecord(record);
                                        messagesProcessed.get(currentPrefix).incrementAndGet();
                                    } catch (Exception e) {
                                        System.err.printf("[%s] Loi xu ly record: %s%n", 
                                            currentPrefix, e.getMessage());
                                        e.printStackTrace();
                                    }
                                });
                            }
                        }
                    } catch (Exception e) {
                        System.err.printf("[%s] Loi trong CDC service: %s%n", 
                                        currentPrefix, e.getMessage());
                        e.printStackTrace();
                    }
                }, currentPrefix + "-cdc-service").start();
            }

            // Đợi tín hiệu tắt
            shutdownLatch.await();
            
            // Thực hiện tắt an toàn
            if (isShuttingDown) {
                System.out.println("Bat dau qua trinh tat an toan...");
                // Tắt các worker pool
                for (ExecutorService pool : workerPools.values()) {
                    pool.shutdown();
                    try {
                        if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                            pool.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        pool.shutdownNow();
                    }
                }
                scheduler.shutdown();
                kafkaService.shutdown();
                aerospikeService.shutdown();
            }
            
            System.out.println("Da tat thanh cong.");
        } catch (Exception e) {
            System.err.println("Loi nghiem trong: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
