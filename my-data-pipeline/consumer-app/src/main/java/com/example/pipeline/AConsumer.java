package com.example.pipeline;

import com.example.pipeline.service.config.ConsumerConfig;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.AerospikeService;
import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.MessageService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Collections;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class AConsumer {
    private static volatile boolean isShuttingDown = false;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private static ConfigurationService configService;
    private static AerospikeService aerospikeService;
    private static KafkaConsumerService kafkaService;
    private static Map<String, MessageService> messageServices;

    public static void main(String[] args, int workerPoolSize, int maxMessagesPerSecond,
                          String sourceHost, int sourcePort, String sourceNamespace,
                          String destinationHost, int destinationPort, 
                          String kafkaBroker) {
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

            // Khởi tạo các message service
            messageServices = new HashMap<>();
            for (Map.Entry<String, String> entry : kafkaService.getPrefixToTopicMap().entrySet()) {
                String prefix = entry.getKey();
                String topic = entry.getValue();
                
                // Tạo cấu hình consumer
                ConsumerConfig consumerConfig = new ConsumerConfig();
                consumerConfig.setName("consumer" + prefix);
                consumerConfig.setNamespace("consumer_" + prefix);
                consumerConfig.setSet("users");
                
                // Tạo Kafka consumer thông qua KafkaConsumerService
                String consumerGroup = prefix + "-group";
                KafkaConsumer<byte[], byte[]> consumer = kafkaService.createConsumer(topic, consumerGroup);
                
                // Đăng ký nhận message từ topic
                String mirroredTopic = "source-kafka." + topic;
                consumer.subscribe(Collections.singletonList(mirroredTopic));
                
                MessageService messageService = new MessageService(
                    aerospikeService.getClient(),
                    aerospikeService.getWritePolicy(),
                    consumerConfig.getNamespace(),
                    consumerConfig.getSet(),
                    prefix,
                    consumer,
                    workerPoolSize
                );
                messageServices.put(prefix, messageService);
                
                // Khởi chạy message service trong một thread riêng
                new Thread(() -> {
                    try {
                        messageService.start();
                    } catch (Exception e) {
                        System.err.printf("[%s] Loi trong message service: %s%n", 
                                        prefix, e.getMessage());
                        e.printStackTrace();
                    }
                }, prefix + "-message-service").start();
            }

            // Đợi tín hiệu tắt
            shutdownLatch.await();
            
            // Thực hiện tắt an toàn
            if (isShuttingDown) {
                System.out.println("Bat dau qua trinh tat an toan...");
                for (MessageService service : messageServices.values()) {
                    service.shutdown();
                }
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
