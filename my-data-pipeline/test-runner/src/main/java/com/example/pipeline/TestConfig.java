package com.example.pipeline;

import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.config.ProducerConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestConfig {
    public static void main(String[] args) {
        // Lấy đối tượng Config
        Config config = ConfigLoader.getConfig();

        // Lấy danh sách producers và prefix_mapping
        List<ProducerConfig> producers = config.getProducers();
        Map<String, List<String>> prefixMapping = config.getPrefix_mapping();

        // Sử dụng Set để tránh trùng lặp tên topic
        Set<String> generatedTopics = new HashSet<>();

        // Tạo tên topic từ danh sách producers và prefix_mapping
        System.out.println("Generated Topics:");
        prefixMapping.forEach((prefix, consumerNames) -> {
            producers.forEach(producer -> {
                // Tạo tên topic
                String topic = TopicNameGenerator.generateTopicName(producer.getName(), prefix);

                // Kiểm tra và thêm vào Set nếu chưa tồn tại
                if (generatedTopics.add(topic)) {
                    System.out.println("Topic: " + topic);

                    // Kiểm tra consumer nào phù hợp với topic này
                    System.out.println("Consumers for topic " + topic + ": " + consumerNames);
                }
            });
        });
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
