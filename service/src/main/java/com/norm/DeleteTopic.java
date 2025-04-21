package com.norm;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DeleteTopic {
    public static void deleteTopic(String bootstrapServers, String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        
        try (AdminClient adminClient = KafkaAdminClient.create(props)) {
            // Xóa topic
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            
            // Đợi cho đến khi xóa hoàn tất
            deleteTopicsResult.all().get(30, TimeUnit.SECONDS);
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