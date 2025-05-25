package com.example.pipeline.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.example.pipeline.service.config.ConfigurationService;
import com.example.pipeline.service.config.ConsumerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaConsumerService {
    private final String kafkaBroker;
    private final AdminClient adminClient;
    private final AtomicLong lastProcessedOffset;
    private final AtomicLong currentOffset;
    private final Map<String, String> prefixToTopicMap;
    private final ConfigurationService configService;

    public KafkaConsumerService(String kafkaBroker, ConfigurationService configService) {
        this.kafkaBroker = kafkaBroker;
        this.configService = configService;
        this.lastProcessedOffset = new AtomicLong(-1);
        this.currentOffset = new AtomicLong(-1);
        this.prefixToTopicMap = configService.getAllPrefixToTopicMappings();

        // Initialize AdminClient
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafkaBroker);
        this.adminClient = AdminClient.create(adminProps);
    }

    public void initializeConsumers(String sourceNamespace, int workerPoolSize) {
        // Initialize prefix to topic mappings
        for (Map.Entry<String, List<String>> entry : configService.getPrefixMappings().entrySet()) {
            String prefix = entry.getKey();
            List<String> consumerNames = entry.getValue();
            
            if (consumerNames.isEmpty()) {
                System.err.println("Warning: No consumers found for prefix " + prefix);
                continue;
            }

            String consumerName = consumerNames.get(0);
            ConsumerConfig consumerConfig = configService.getConsumerConfig(consumerName);
            
            if (consumerConfig == null) {
                System.err.println("Warning: No consumer config found for " + consumerName);
                continue;
            }

            // Topic mapping is already initialized in constructor
            if (!prefixToTopicMap.containsKey(prefix)) {
                System.err.println("Warning: No topic mapping found for prefix " + prefix);
            }
        }
    }

    public KafkaConsumer<byte[], byte[]> createConsumer(String topic, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "100");
        props.put("max.poll.records", "5000");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
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

    public Map<String, String> getPrefixToTopicMap() {
        return prefixToTopicMap;
    }

    public void shutdown() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
} 