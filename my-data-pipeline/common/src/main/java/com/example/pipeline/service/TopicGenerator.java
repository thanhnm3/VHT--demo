package com.example.pipeline.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.Config.Producer;

public class TopicGenerator {

    // Phương thức trả về danh sách các topic
    public static Map<String, String> generateTopics() {
        Map<String, String> topicMap = new HashMap<>();
        try {
            // Lấy đối tượng Config
            Config config = ConfigLoader.getConfig();

            // Lấy danh sách producers và prefix_mapping
            List<Producer> producers = config.getProducers();
            Map<String, List<String>> prefixMapping = config.getPrefix_mapping();

            // Tạo tên topic từ danh sách producers và prefix_mapping
            prefixMapping.forEach((prefix, consumerNames) -> {
                producers.forEach(producer -> {
                    // Tạo tên topic
                    String topic = TopicNameGenerator.generateTopicName(producer.getName(), prefix);

                    // Lưu tên topic vào Map
                    topicMap.put(prefix, topic);
                });
            });
        } catch (Exception e) {
            System.err.println("Error generating topics: " + e.getMessage());
            e.printStackTrace();
        }
        return topicMap;
    }

    // Tạo tên topic cho CDC consumer
    public static String generateCdcTopicName(String baseTopic) {
        return baseTopic + "-cdc";
    }

    // Tạo tên consumer group cho CDC
    public static String generateCdcGroupName(String baseTopic) {
        // Nếu baseTopic đã có hậu tố -cdc thì chỉ thêm -group
        if (baseTopic.endsWith("-cdc")) {
            return baseTopic + "-group";
        }
        // Nếu chưa có hậu tố -cdc thì thêm vào trước -group
        return baseTopic + "-cdc-group";
    }

    // Tạo tên topic cho A consumer
    public static String generateATopicName(String baseTopic) {
        return baseTopic + "-a";
    }

    // Tạo tên consumer group cho A
    public static String generateAGroupName(String baseTopic) {
        return baseTopic + "-a-group";
    }

    // Tạo tên topic cho mirrored topic
    public static String generateMirroredTopicName(String topic) {
        return "source-kafka." + topic;
    }

    public static class TopicNameGenerator {

        public static String generateTopicName(String producerName, String prefix) {
            if (prefix == null || prefix.length() < 3) {
                throw new IllegalArgumentException("Invalid prefix: " + prefix);
            }

            return String.format("%s_%s", normalize(producerName), prefix);
        }

        private static String normalize(String name) {
            // Replace dots, spaces, or special characters to ensure Kafka topic-safe name
            return name.toLowerCase().replaceAll("[^a-z0-9]", "_");
        }
    }
}