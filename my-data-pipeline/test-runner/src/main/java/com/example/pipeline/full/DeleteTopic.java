package com.example.pipeline.full;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.util.Properties;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DeleteTopic {
    public static void deleteAllTopics(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            Set<String> allTopics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);

            if (allTopics.isEmpty()) {
                System.out.println("Khong co topic nao de xoa.");
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
                    System.out.println("Da xoa topic: " + topic);
                } catch (Exception e) {
                    System.err.println("Loi khi xoa topic " + topic + ": " + e.getMessage());
                }
            }
            System.out.println("Da xoa xong cac topic khong can thiet.");
        } catch (Exception e) {
            System.err.println("Loi khi xoa topic: " + e.getMessage());
            e.printStackTrace();
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
            System.out.println("Topic " + topicName + " da duoc danh dau de xoa.");

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
                System.err.println("Khong the xoa topic " + topicName + " sau " + retries + " lan thu.");
                return;
            }

            System.out.println("Topic " + topicName + " da duoc xoa thanh cong.");
            
            // Tạo lại topic mới
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get(30, TimeUnit.SECONDS);
            System.out.println("Topic " + topicName + " da duoc tao lai thanh cong.");
            
        } catch (Exception e) {
            System.err.println("Loi khi xoa/tao topic: " + e.getMessage());
            e.printStackTrace();
        }
    }
}