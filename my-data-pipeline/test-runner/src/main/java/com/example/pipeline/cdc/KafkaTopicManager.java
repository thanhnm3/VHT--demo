package com.example.pipeline.cdc;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KafkaTopicManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicManager.class);
    private final AdminClient adminClient;
    private final List<String> topics;

    public KafkaTopicManager(AdminClient adminClient, List<String> topics) {
        this.adminClient = adminClient;
        this.topics = topics;
    }

    public void createTopics() {
        try {
            // Create topics for each region
            for (String topic : topics) {
                logger.info("Creating/Verifying topic: {}", topic);
                
                // Create topic with 2 partitions and replication factor 2
                NewTopic newTopic = new NewTopic(topic, 2, (short) 2);
                CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
                result.all().get(30, TimeUnit.SECONDS);
                
                logger.info("Created/Verified topic: {}", topic);
            }
        } catch (Exception e) {
            logger.error("Error creating topics: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create topics", e);
        }
    }
} 