package com.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class StringProducer {
    public static void main(String[] args) {
        // Cấu hình Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092"); // Kafka nguồn
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Tạo Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Gửi 100 thông điệp
        try {
            for (int i = 1; i <= 100; i++) {
                String message = "Message " + i;

                // Gửi thông điệp với callback để xử lý lỗi
                producer.send(new ProducerRecord<>("test-topic", null, message), (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        // In ra lỗi nếu có
                        System.err.println("❌ Lỗi khi gửi thông điệp: " + exception.getMessage());
                        exception.printStackTrace();
                    } else {
                        // In ra thông tin nếu gửi thành công
                        System.out.printf("✅ Đã gửi thông điệp tới topic %s partition %d offset %d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });

                // Tạm dừng 100ms giữa các lần gửi (nếu cần)
                Thread.sleep(100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}