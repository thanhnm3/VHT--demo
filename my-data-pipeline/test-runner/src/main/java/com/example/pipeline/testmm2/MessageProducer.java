package com.example.pipeline.testmm2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MessageProducer {
    private static final String TOPIC_NAME = "demo-topic";
    private static final String SOURCE_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TARGET_BOOTSTRAP_SERVERS = "localhost:9093";
    private static final String SOURCE_CLUSTER = "source-kafka";

    public static void main(String[] args) {
        // 1. Gửi message
        String messageId = sendMessage();
        if (messageId == null) {
            System.out.println("Không thể gửi message!");
            return;
        }

        // 2. Kiểm tra message trong target
        checkMessageInTarget(messageId);
    }

    private static String sendMessage() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String messageId = "msg-" + System.currentTimeMillis();
            String message = "Test message " + messageId;
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageId, message);
            
            producer.send(record).get();
            System.out.println("Đã gửi message: " + message);
            return messageId;
        } catch (Exception e) {
            System.err.println("Lỗi khi gửi message: " + e.getMessage());
            return null;
        }
    }

    private static void checkMessageInTarget(String messageId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TARGET_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String mirroredTopic = SOURCE_CLUSTER + "." + TOPIC_NAME;
        System.out.println("\nKiểm tra message trong topic: " + mirroredTopic);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(mirroredTopic));
            
            // Đợi tối đa 10 giây
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 10000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Nhận được message: " + record.value());
                    if (record.key().equals(messageId)) {
                        System.out.println("✓ Tìm thấy message trong target Kafka!");
                        return;
                    }
                }
            }
            System.out.println("✗ Không tìm thấy message sau 10 giây");
        } catch (Exception e) {
            System.err.println("Lỗi khi kiểm tra message: " + e.getMessage());
        }
    }
} 