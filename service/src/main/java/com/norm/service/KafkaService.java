package com.norm.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class KafkaService {
    private final String kafkaBroker;
    private final String kafkaTopic;
    private final String consumerGroup;
    private final AdminClient adminClient;
    private final AtomicLong lastProcessedOffset;
    private final AtomicLong currentOffset;

    public KafkaService(String kafkaBroker, String kafkaTopic, String consumerGroup) {
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

    public KafkaProducer<byte[], byte[]> createProducer(int maxRetries) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(maxRetries));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "134217728");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10485760");

        return new KafkaProducer<>(props);
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

    public long calculateTotalLag() {
        try {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(consumerGroup);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets = result.partitionsToOffsetAndMetadata().get();
            
            List<TopicPartition> partitions = offsets.keySet().stream()
                .filter(tp -> tp.topic().equals(kafkaTopic))
                .collect(Collectors.toList());
                
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = adminClient.listOffsets(
                partitions.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))
            ).all().get();
            
            long totalLag = 0;
            for (TopicPartition partition : partitions) {
                long endOffset = endOffsets.get(partition).offset();
                long currentOffset = offsets.get(partition).offset();
                totalLag += (endOffset - currentOffset);
            }
            return totalLag;
        } catch (Exception e) {
            System.err.println("Error calculating total lag: " + e.getMessage());
            return 0;
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

    public void shutdown() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
} 