package com.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StringProducer {
    public static void main(String[] args) {
        // Cấu hình Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka nguồn
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Tạo Kafka Producer
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        
        // Tạo CountDownLatch để đợi tất cả message được gửi
        CountDownLatch latch = new CountDownLatch(10);

        // Gửi 10 thông điệp
        try {
            for (int i = 1; i <= 10; i++) {
                // Chuyển đổi key và value thành byte[]
                byte[] key = String.valueOf(i).getBytes();
                byte[] value = ("message " + i).getBytes();

                // Gửi thông điệp với callback để xử lý lỗi
                producer.send(new ProducerRecord<>("test-topic", key, value), (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        // In ra lỗi nếu có
                        System.err.println("Loi khi gui thong diep: " + exception.getMessage());
                        exception.printStackTrace();
                    } else {
                        // In ra thông tin nếu gửi thành công
                        System.out.printf("Da gui thong diep tới topic %s partition %d offset %d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
                    latch.countDown();
                });

                // Tạm dừng 100ms giữa các lần gửi (nếu cần)
                Thread.sleep(100);
            }
            
            // Đợi tất cả message được gửi xong
            latch.await();
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Đảm bảo tất cả message được gửi trước khi đóng
            producer.flush();
            producer.close();
        }
    }
}