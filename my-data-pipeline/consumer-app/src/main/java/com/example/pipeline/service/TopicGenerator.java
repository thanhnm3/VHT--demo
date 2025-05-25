package com.example.pipeline.service;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.ProducerConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicGenerator {
    public static Map<String, String> generateTopics() {
        Map<String, String> topicMap = new HashMap<>();
        try {
            // Lấy đối tượng Config
            Config config = ConfigLoader.getConfig();

            // Lấy danh sách producers và prefix_mapping
            List<ProducerConfig> producers = config.getProducers();
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