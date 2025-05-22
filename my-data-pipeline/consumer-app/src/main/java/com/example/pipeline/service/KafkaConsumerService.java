package com.example.pipeline.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;


public class KafkaConsumerService {
    private final String kafkaBroker;
    private final String kafkaTopic;
    private final String consumerGroup;
    private final AdminClient adminClient;
    private final AtomicLong lastProcessedOffset;
    private final AtomicLong currentOffset;

    public KafkaConsumerService(String kafkaBroker, String kafkaTopic, String consumerGroup) {
        this.kafkaBroker = kafkaBroker;
        this.kafkaTopic = kafkaTopic;
        this.consumerGroup = consumerGroup;
        this.lastProcessedOffset = new AtomicLong(-1);
        this.currentOffset = new AtomicLong(-1);

        // Initialize AdminClient
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        this.adminClient = AdminClient.create(adminProps);
    }


    public KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(kafkaTopic));
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

    public void shutdown() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
} 