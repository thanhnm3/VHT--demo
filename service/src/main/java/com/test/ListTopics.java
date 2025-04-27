package com.test;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ListTopics {
    public static void main(String[] args) {
        // Cấu hình AdminClient
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Tạo AdminClient
        try (AdminClient adminClient = AdminClient.create(props)) {
            // Lấy danh sách topics
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get();

            // In ra danh sách topics
            System.out.println("📋 Danh sách các topic trong Kafka:");
            System.out.println("--------------------------------");
            for (String topic : topicNames) {
                System.out.println("• " + topic);
            }
            System.out.println("--------------------------------");
            System.out.println("Tổng số topic: " + topicNames.size());

        } catch (InterruptedException | ExecutionException e) {
            System.err.println("❌ Lỗi khi lấy danh sách topics: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 