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
            // Lấy danh sách tất cả các topic
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get(5, TimeUnit.SECONDS);
            
            if (topicNames.isEmpty()) {
                System.out.println("Khong co topic nao de xoa.");
                return;
            }

            // Xóa tất cả các topic
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicNames);
            deleteTopicsResult.all().get(30, TimeUnit.SECONDS);
            System.out.println("Da danh dau xoa " + topicNames.size() + " topic.");

            // Đợi tất cả các topic được xóa hoàn toàn
            boolean topicsExist = true;
            int retries = 0;
            while (topicsExist && retries < 10) {
                Thread.sleep(1000); // Đợi 1 giây
                topics = adminClient.listTopics();
                Set<String> remainingTopics = topics.names().get(5, TimeUnit.SECONDS);
                topicsExist = !remainingTopics.isEmpty();
                retries++;
            }

            if (topicsExist) {
                System.err.println("Khong the xoa tat ca topic sau " + retries + " lan thu.");
                return;
            }

            System.out.println("Da xoa thanh cong tat ca " + topicNames.size() + " topic.");
            
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