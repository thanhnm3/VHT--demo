package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaTestProducer {
    public static void main(String[] args) {
        // Cấu hình Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Địa chỉ Kafka Broker
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Tạo Kafka Producer
        Producer<String, String> producer = new KafkaProducer<>(props);
        
        String topic = "test-topic";
        String key = "key1";
        String value = "Hello, Kafka!";
        
        // Gửi tin nhắn
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
        
        // Đóng Producer
        producer.close();
        
        System.out.println("Message sent successfully!");
    }
}

