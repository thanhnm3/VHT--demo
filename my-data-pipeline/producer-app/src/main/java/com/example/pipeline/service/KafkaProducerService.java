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


public class KafkaProducerService {
    private final String kafkaBroker;
    private final String kafkaTopic;
    private final String consumerGroup;
    private final AdminClient adminClient;


    public KafkaProducerService(String kafkaBroker, String kafkaTopic, String consumerGroup) {
        this.kafkaBroker = kafkaBroker;
        this.kafkaTopic = kafkaTopic;
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

        return new KafkaProducer<>(props);
    }

        public void createTopic(String topicName) {
        // Logic để tạo topic trong Kafka
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

    public void shutdown() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }
}
