package com.example.pipeline.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.HashSet;


public class KafkaProducerService {
    private final String kafkaBroker;
    private final Set<String> kafkaTopics;
    private final String consumerGroup;
    private final AdminClient adminClient;


    public KafkaProducerService(String kafkaBroker, String topics, String consumerGroup) {
        this.kafkaBroker = kafkaBroker;
        this.kafkaTopics = new HashSet<>();
        if (topics != null && !topics.isEmpty()) {
            this.kafkaTopics.addAll(List.of(topics.split(",")));
        }
        this.consumerGroup = consumerGroup;


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
        
        // Thêm các cấu hình mới để đảm bảo an toàn khi broker bị tắt
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000"); // Thời gian chờ giữa các lần retry
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000"); // Timeout cho mỗi request
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); // Timeout cho việc gửi message
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000"); // Timeout cho các operation blocking

        return new KafkaProducer<>(props);
    }

    public void createTopic(String topicName) {
        try {
            // Kiểm tra xem topic đã tồn tại chưa
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (existingTopics.contains(topicName)) {
                System.out.println("Topic " + topicName + " already exists");
                return;
            }

            // Tạo cấu hình cho topic mới
            org.apache.kafka.clients.admin.NewTopic newTopic = new org.apache.kafka.clients.admin.NewTopic(
                topicName,
                2,  // số partition
                (short) 2  // replication factor
            );

            // Tạo topic
            adminClient.createTopics(java.util.Collections.singleton(newTopic)).all().get();
            System.out.println("Created topic " + topicName + " with 2 partitions and replication factor 2");
        } catch (Exception e) {
            System.err.println("Error creating topic " + topicName + ": " + e.getMessage());
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
    }

    public long calculateTotalLag() {
        try {
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(consumerGroup);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets = result.partitionsToOffsetAndMetadata().get();
            
            List<TopicPartition> partitions = offsets.keySet().stream()
                .filter(tp -> kafkaTopics.contains(tp.topic()))
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

    public void shutdown() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
}
