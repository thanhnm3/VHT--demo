package com.example.pipeline.full;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class TopicManager {
    private static final Logger logger = LoggerFactory.getLogger(TopicManager.class);

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            String kafkaBroker = config.getKafka().getBroker();
            logger.info("=== Topic Manager ===");
            logger.info("Kafka Broker: {}", kafkaBroker);

            // Xóa tất cả topic
            deleteAllTopics(kafkaBroker);

            // Tạo lại các topic mới
            recreateTopics(kafkaBroker, config);

            logger.info("=== Topic Manager Completed ===");
        } catch (Exception e) {
            logger.error("Error in Topic Manager: {}", e.getMessage(), e);
        }
    }

    private static void deleteAllTopics(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            Set<String> allTopics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);

            if (allTopics.isEmpty()) {
                logger.info("No topics to delete.");
                return;
            }

            for (String topic : allTopics) {
                if (isSystemTopic(topic)) {
                    continue; // Skip system topics
                }
                try {
                    adminClient.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
                    logger.info("Deleted topic: {}", topic);
                } catch (Exception e) {
                    logger.error("Error deleting topic {}: {}", topic, e.getMessage());
                }
            }
            logger.info("Finished deleting unnecessary topics.");
        } catch (Exception e) {
            logger.error("Error in deleteAllTopics: {}", e.getMessage(), e);
        }
    }

    private static void recreateTopics(String bootstrapServers, Config config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            Config.Producer producer = config.getProducers().get(0);
            List<NewTopic> newTopics = new ArrayList<>();

            // Tạo topic cho mỗi prefix
            for (String prefix : config.getPrefix_mapping().keySet()) {
                // Tạo topic cho A
                String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producer.getName(), prefix);
                String aTopic = TopicGenerator.generateATopicName(baseTopic);
                newTopics.add(new NewTopic(aTopic, 1, (short) 1));

                // Tạo topic cho CDC
                String cdcTopic = TopicGenerator.generateCdcTopicName(baseTopic);
                newTopics.add(new NewTopic(cdcTopic, 1, (short) 1));
            }

            // Tạo tất cả topic
            adminClient.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
            logger.info("Successfully created all topics");
        } catch (Exception e) {
            logger.error("Error in recreateTopics: {}", e.getMessage(), e);
        }
    }

    private static boolean isSystemTopic(String topic) {
        return topic.equals("mm2-status") ||
               topic.equals("mm2-offsets") ||
               topic.equals("mm2-configs") ||
               topic.equals("source-kafka.heartbeats");
    }
} 