package com.example.pipeline.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.config.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final String kafkaBroker;
    private final AdminClient adminClient;
    private final AtomicLong lastProcessedOffset;
    private final AtomicLong currentOffset;
    private final Map<String, String> regionToTopicMap;
    private final ConfigurationService configService;
    private volatile boolean isRunning = true;
    private KafkaConsumer<byte[], byte[]> consumer;

    public KafkaConsumerService(String kafkaBroker, ConfigurationService configService) {
        this.kafkaBroker = kafkaBroker;
        this.configService = configService;
        this.lastProcessedOffset = new AtomicLong(-1);
        this.currentOffset = new AtomicLong(-1);
        this.regionToTopicMap = configService.getAllRegionToTopicMappings();

        // Initialize AdminClient
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafkaBroker);
        this.adminClient = AdminClient.create(adminProps);
    }

    public void initializeConsumers(String sourceNamespace, int workerPoolSize) {
        // Initialize region to topic mappings
        for (Map.Entry<String, List<String>> entry : configService.getRegionMappings().entrySet()) {
            String region = entry.getKey();
            List<String> consumerNames = entry.getValue();
            
            if (consumerNames.isEmpty()) {
                logger.warn("No consumers found for region {}", region);
                continue;
            }

            String consumerName = consumerNames.get(0);
            Config.Consumer consumerConfig = configService.getConsumerConfig(consumerName);
            
            if (consumerConfig == null) {
                logger.warn("No consumer config found for {}", consumerName);
                continue;
            }

            // Topic mapping is already initialized in constructor
            if (!regionToTopicMap.containsKey(region)) {
                logger.warn("No topic mapping found for region {}", region);
            }
        }
    }

    public KafkaConsumer<byte[], byte[]> createConsumer(String topic, String groupId) {
        try {
            // Kiểm tra topic có tồn tại không
            if (!adminClient.listTopics().names().get().contains(topic)) {
                logger.info("Topic {} does not exist, creating it...", topic);
                // Tạo topic mới với 2 partitions và replication factor 2
                NewTopic newTopic = new NewTopic(topic, 2, (short) 2);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                logger.info("Successfully created topic {}", topic);
            }

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaBroker);
            props.put("group.id", groupId);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put("auto.offset.reset", "earliest");
            props.put("enable.auto.commit", "false");
            props.put("max.poll.records", "10000");
            props.put("fetch.min.bytes", "1048576"); // 1MB
            props.put("fetch.max.wait.ms", "500");
            props.put("max.poll.interval.ms", "300000");
            props.put("session.timeout.ms", "30000");
            props.put("heartbeat.interval.ms", "10000");
            props.put("max.partition.fetch.bytes", "1048576"); // 1MB
            props.put("receive.buffer.bytes", "32768");
            props.put("send.buffer.bytes", "131072");
            props.put("request.timeout.ms", "30000");
            props.put("retry.backoff.ms", "100");
            props.put("reconnect.backoff.ms", "50");
            props.put("reconnect.backoff.max.ms", "1000");

            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
            logger.info("Successfully subscribed consumer group {} to topic {}", groupId, topic);
            return consumer;
        } catch (Exception e) {
            logger.error("Failed to create consumer for topic {} and group {}: {}", topic, groupId, e.getMessage());
            throw new RuntimeException("Failed to create consumer", e);
        }
    }

    public void startConsuming(String topic, String groupId, MessageService messageService) {
        try {
            logger.info("Starting consumer for topic: {} with group: {}", topic, groupId);
            
            // Create consumer properties
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

            // Create consumer
            consumer = new KafkaConsumer<>(props);
            
            // Subscribe to topic
            consumer.subscribe(Collections.singletonList(topic));
            logger.info("Successfully subscribed consumer group {} to topic {}", groupId, topic);

            // Start consuming in a separate thread
            new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                        
                        if (!records.isEmpty()) {
                            messageService.processRecords(records);
                            consumer.commitSync();
                            logger.debug("Committed offset for {} records", records.count());
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error consuming messages: {}", e.getMessage(), e);
                }
            }, "consumer-" + groupId).start();

        } catch (Exception e) {
            logger.error("Error starting consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to start consumer", e);
        }
    }

    public void updateOffsets(long currentOffset, long lastProcessedOffset) {
        this.currentOffset.set(currentOffset);
        this.lastProcessedOffset.set(lastProcessedOffset);
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public long getLastProcessedOffset() {
        return lastProcessedOffset.get();
    }

    public Map<String, String> getRegionToTopicMap() {
        return regionToTopicMap;
    }

    public void shutdown() {
        isRunning = false;
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
} 