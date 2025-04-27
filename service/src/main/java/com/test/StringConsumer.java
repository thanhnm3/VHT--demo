package com.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StringConsumer {
    public static void main(String[] args) {
        // Cấu hình Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:39092"); // Kafka đích
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "string-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Đọc từ đầu topic

        // Tạo Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Đăng ký topic
        consumer.subscribe(Collections.singletonList("test-topic"));

        System.out.println("🧭 Đang lắng nghe topic 'test-topic'...");

        try {
            
                // Lấy các bản ghi từ topic
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    // In ra thông điệp
                    System.out.printf("⬅️  Nhận thông điệp từ partition %d offset %d: %s%n",
                            record.partition(), record.offset(), record.value());
                }
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}