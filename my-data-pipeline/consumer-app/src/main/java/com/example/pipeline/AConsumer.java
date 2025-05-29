package com.example.pipeline;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.AerospikeService;
import com.example.pipeline.service.KafkaConsumerService;
import com.example.pipeline.service.MessageService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class AConsumer {
    private static final Logger logger = LoggerFactory.getLogger(AConsumer.class);
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
                logger.info("Nhan tin hieu tat. Bat dau qua trinh tat an toan...");
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
            Map<String, List<String>> prefixMapping = configService.getPrefixMappings();
            Map<String, String> prefixToTopicMap = kafkaService.getPrefixToTopicMap();
            
            for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                String prefix = entry.getKey();
                List<String> consumerNames = entry.getValue();
                
                if (consumerNames.isEmpty()) {
                    logger.warn("No consumers found for prefix {}", prefix);
                    continue;
                }

                // Get the first consumer for this prefix
                String consumerName = consumerNames.get(0);
                Config.Consumer consumer = configService.getConsumerConfig(consumerName);
                if (consumer == null) {
                    logger.warn("No consumer config found for {}", consumerName);
                    continue;
                }

                String topic = prefixToTopicMap.get(prefix);
                if (topic == null) {
                    logger.warn("No topic found for prefix {}", prefix);
                    continue;
                }
                
                // Tạo Kafka consumer thông qua KafkaConsumerService
                String consumerGroup = configService.getConsumerGroup(consumerName);
                KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaService.createConsumer(topic, consumerGroup);
                
                // Đăng ký nhận message từ topic
                String mirroredTopic = "source-kafka." + topic;
                kafkaConsumer.subscribe(Collections.singletonList(mirroredTopic));
                
                MessageService messageService = new MessageService(
                    aerospikeService.getClient(),
                    aerospikeService.getWritePolicy(),
                    consumer.getNamespace(),
                    consumer.getSet(),
                    prefix,
                    kafkaConsumer,
                    workerPoolSize
                );
                messageServices.put(prefix, messageService);
                
                // Khởi chạy message service trong một thread riêng
                new Thread(() -> {
                    try {
                        messageService.start();
                    } catch (Exception e) {
                        logger.error("[{}] Loi trong message service: {}", prefix, e.getMessage(), e);
                    }
                }, prefix + "-message-service").start();
            }

            // Đợi tín hiệu tắt
            shutdownLatch.await();
            
            // Thực hiện tắt an toàn
            if (isShuttingDown) {
                logger.info("Bat dau qua trinh tat an toan...");
                for (MessageService service : messageServices.values()) {
                    service.shutdown();
                }
                kafkaService.shutdown();
                aerospikeService.shutdown();
            }
            
            logger.info("Da tat thanh cong.");
        } catch (Exception e) {
            logger.error("Loi nghiem trong: {}", e.getMessage(), e);
        }
    }
}
