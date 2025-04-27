package com.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StringConsumer {
    public static void main(String[] args) {
        // Cấu hình Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093"); // Target Kafka
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Bắt đầu đọc từ đầu topic
        
        // Thêm các cấu hình để xử lý message đúng cách
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // Tạo Kafka Consumer
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);

        try {
            // Subscribe vào topic đã được replicate
            consumer.subscribe(Collections.singletonList("source-kafka.test-topic"));
            System.out.println("Da subscribe vao topic source-kafka.test-topic tren target Kafka");

            // Bắt đầu consume messages
            while (true) {
                // Poll messages với timeout 100ms
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

                // Xử lý từng record
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    // Hiển thị dạng byte
                    System.out.println("=== Dang byte ===");
                    System.out.println("Key (byte): " + bytesToHex(record.key()));
                    System.out.println("Value (byte): " + bytesToHex(record.value()));
                    
                    // Hiển thị dạng String
                    System.out.println("=== Dang String ===");
                    System.out.println("Key (String): " + new String(record.key()));
                    System.out.println("Value (String): " + new String(record.value()));
                    
                    // Hiển thị thông tin khác
                    System.out.println("=== Thong tin khac ===");
                    System.out.printf("Topic: %s, Partition: %d, Offset: %d%n",
                            record.topic(),
                            record.partition(),
                            record.offset());
                    System.out.println("----------------------------------------");
                }
            }
        } catch (Exception e) {
            System.err.println("Loi khi consume messages: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    // Hàm chuyển đổi byte[] thành chuỗi hex
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }
}