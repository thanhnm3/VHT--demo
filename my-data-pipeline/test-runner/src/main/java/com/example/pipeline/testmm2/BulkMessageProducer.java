package com.example.pipeline.testmm2;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class BulkMessageProducer {
    private static final String TOPIC_NAME = "demo-topic";
    private static final String SOURCE_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TARGET_BOOTSTRAP_SERVERS = "localhost:9093";
    private static final String SOURCE_CLUSTER = "source-kafka";
    private static final int MESSAGE_COUNT = 100;

    public static void main(String[] args) {
        // 1. Gửi 100 message
        Set<String> sentKeys = sendMessages();

        // 2. Kiểm tra message trong target
        checkMessagesInTarget(sentKeys);
    }

    private static Set<String> sendMessages() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Set<String> keys = new HashSet<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= MESSAGE_COUNT; i++) {
                String key = String.valueOf(i);
                String value = String.valueOf(i);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
                producer.send(record).get();
                keys.add(key);
                System.out.println("Đã gửi message: key=" + key + ", value=" + value);
            }
        } catch (Exception e) {
            System.err.println("Lỗi khi gửi message: " + e.getMessage());
        }
        return keys;
    }

    private static void checkMessagesInTarget(Set<String> sentKeys) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TARGET_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-bulk");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String mirroredTopic = SOURCE_CLUSTER + "." + TOPIC_NAME;
        System.out.println("\nKiểm tra các message trong topic: " + mirroredTopic);

        Set<String> foundKeys = new HashSet<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(mirroredTopic));
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 15000 && foundKeys.size() < sentKeys.size()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    if (sentKeys.contains(record.key())) {
                        foundKeys.add(record.key());
                        System.out.println("Tìm thấy message: key=" + record.key() + ", value=" + record.value());
                    }
                }
            }
            System.out.println("\nTổng số message đã gửi: " + sentKeys.size());
            System.out.println("Tổng số message tìm thấy ở Kafka đích: " + foundKeys.size());
            if (foundKeys.size() == sentKeys.size()) {
                System.out.println("✓ Tất cả message đã được replicate thành công!");
            } else {
                sentKeys.removeAll(foundKeys);
                System.out.println("✗ Các key chưa tìm thấy: " + sentKeys);
            }
        } catch (Exception e) {
            System.err.println("Lỗi khi kiểm tra message: " + e.getMessage());
        }
    }
}
