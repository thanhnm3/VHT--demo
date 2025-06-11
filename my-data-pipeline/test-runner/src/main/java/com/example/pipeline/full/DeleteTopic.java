package com.example.pipeline.full;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DeleteTopic {
    private static final Logger logger = LoggerFactory.getLogger(DeleteTopic.class);

    public static void deleteAllTopics(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            Set<String> allTopics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);

            if (allTopics.isEmpty()) {
                logger.info("Khong co topic nao de xoa.");
                return;
            }

            for (String topic : allTopics) {
                if (
                    topic.equals("mm2-status") ||
                    topic.equals("mm2-offsets") ||
                    topic.equals("mm2-configs") ||
                    topic.equals("source-kafka.heartbeats")
                ) {
                    continue; // Không xóa các topic này
                }
                try {
                    adminClient.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
                    logger.info("Da xoa topic: {}", topic);
                } catch (Exception e) {
                    logger.error("Loi khi xoa topic {}: {}", topic, e.getMessage());
                }
            }
            logger.info("Da xoa xong cac topic khong can thiet.");
        } catch (Exception e) {
            logger.error("Loi khi xoa topic: {}", e.getMessage(), e);
        }
    }

    // Giữ lại phương thức cũ để tương thích ngược
    public static void deleteTopic(String bootstrapServers, String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            // Xóa topic
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            deleteTopicsResult.all().get(30, TimeUnit.SECONDS);
            logger.info("Topic {} da duoc danh dau de xoa.", topicName);

            // Đợi topic được xóa hoàn toàn
            boolean topicExists = true;
            int retries = 0;
            while (topicExists && retries < 10) {
                Thread.sleep(1000); // Đợi 1 giây
                ListTopicsResult topics = adminClient.listTopics();
                Set<String> topicNames = topics.names().get(5, TimeUnit.SECONDS);
                topicExists = topicNames.contains(topicName);
                retries++;
            }

            if (topicExists) {
                logger.error("Khong the xoa topic {} sau {} lan thu.", topicName, retries);
                return;
            }

            logger.info("Topic {} da duoc xoa thanh cong.", topicName);
            
            // Tạo lại topic mới với 2 partitions và replication factor 2
            NewTopic newTopic = new NewTopic(topicName, 2, (short) 2);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get(30, TimeUnit.SECONDS);
            logger.info("Topic {} da duoc tao lai thanh cong voi 2 partitions va replication factor 2.", topicName);
            
        } catch (Exception e) {
            logger.error("Loi khi xoa/tao topic: {}", e.getMessage(), e);
        }
    }
}