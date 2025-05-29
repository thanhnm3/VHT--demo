package com.example.pipeline.service;

import com.example.pipeline.service.config.Config;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaLagMonitor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLagMonitor.class);
    private final AdminClient adminClient;
    private static final int TIMEOUT_SECONDS = 5;

    public KafkaLagMonitor() {
        // Lấy target Kafka broker từ config
        Config config = ConfigLoader.getConfig();
        String targetBroker = config.getKafka().getBrokers().getTarget();
        
        Properties props = new Properties();
        props.put("bootstrap.servers", targetBroker);
        this.adminClient = AdminClient.create(props);
        logger.info("Initialized KafkaLagMonitor with target broker: {}", targetBroker);
    }

    public KafkaLagMonitor(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        this.adminClient = AdminClient.create(props);
        logger.info("Initialized KafkaLagMonitor with custom broker: {}", bootstrapServers);
    }

    /**
     * Tính toán tổng lag của một topic
     * @param topic Tên topic cần tính lag
     * @param consumerGroup Consumer group cần tính lag
     * @return Tổng số message đang lag
     */
    public long calculateTopicLag(String topic, String consumerGroup) {
        try {
            // Lấy consumer group offsets
            ListConsumerGroupOffsetsResult consumerOffsets = adminClient.listConsumerGroupOffsets(consumerGroup);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> consumerOffsetMap = 
                consumerOffsets.partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Lọc các partition của topic cần tính
            Set<TopicPartition> topicPartitions = new HashSet<>();
            for (TopicPartition tp : consumerOffsetMap.keySet()) {
                if (tp.topic().equals(topic)) {
                    topicPartitions.add(tp);
                }
            }

            if (topicPartitions.isEmpty()) {
                logger.warn("No partitions found for topic: {}", topic);
                return 0;
            }

            // Lấy end offsets của topic
            Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
            for (TopicPartition tp : topicPartitions) {
                offsetSpecs.put(tp, OffsetSpec.latest());
            }

            ListOffsetsResult endOffsets = adminClient.listOffsets(offsetSpecs);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsetMap = 
                endOffsets.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Tính tổng lag
            long totalLag = 0;
            for (TopicPartition tp : topicPartitions) {
                long consumerOffset = consumerOffsetMap.get(tp).offset();
                long endOffset = endOffsetMap.get(tp).offset();
                long partitionLag = endOffset - consumerOffset;
                totalLag += partitionLag;
                
                logger.debug("Partition {}: consumer offset = {}, end offset = {}, lag = {}", 
                           tp.partition(), consumerOffset, endOffset, partitionLag);
            }

            logger.info("Total lag for topic {} in consumer group {}: {}", 
                       topic, consumerGroup, totalLag);
            return totalLag;

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Error calculating lag for topic {}: {}", topic, e.getMessage());
            return 0;
        }
    }

    /**
     * Lấy thông tin chi tiết về lag của từng partition
     * @param topic Tên topic
     * @param consumerGroup Consumer group
     * @return Map chứa thông tin lag của từng partition
     */
    public Map<Integer, Long> getPartitionLags(String topic, String consumerGroup) {
        Map<Integer, Long> partitionLags = new HashMap<>();
        try {
            ListConsumerGroupOffsetsResult consumerOffsets = adminClient.listConsumerGroupOffsets(consumerGroup);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> consumerOffsetMap = 
                consumerOffsets.partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Set<TopicPartition> topicPartitions = new HashSet<>();
            for (TopicPartition tp : consumerOffsetMap.keySet()) {
                if (tp.topic().equals(topic)) {
                    topicPartitions.add(tp);
                }
            }

            Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
            for (TopicPartition tp : topicPartitions) {
                offsetSpecs.put(tp, OffsetSpec.latest());
            }

            ListOffsetsResult endOffsets = adminClient.listOffsets(offsetSpecs);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsetMap = 
                endOffsets.all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            for (TopicPartition tp : topicPartitions) {
                long consumerOffset = consumerOffsetMap.get(tp).offset();
                long endOffset = endOffsetMap.get(tp).offset();
                partitionLags.put(tp.partition(), endOffset - consumerOffset);
            }

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Error getting partition lags for topic {}: {}", topic, e.getMessage());
        }
        return partitionLags;
    }

    public void shutdown() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
} 