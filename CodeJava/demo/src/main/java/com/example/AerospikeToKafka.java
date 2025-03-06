package com.example;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AerospikeToKafka {
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "pub";
    private static final String SET_NAME = "users";

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";

    private static AerospikeClient client;
    private static KafkaProducer<String, byte[]> producer;

    public static void main(String[] args) {
        // 1. Kết nối Aerospike
        client = new AerospikeClient(new ClientPolicy(), AEROSPIKE_HOST, AEROSPIKE_PORT);

        // 2. Cấu hình Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        producer = new KafkaProducer<>(props);

        // 3. Dùng ScheduledExecutorService để gửi dữ liệu mỗi giây
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(AerospikeToKafka::sendDataToKafka, 0, 1, TimeUnit.SECONDS);

        // Chạy mãi cho đến khi tắt chương trình
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Đang đóng kết nối...");
            producer.close();
            client.close();
            scheduler.shutdown();
        }));
    }

    private static void sendDataToKafka() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;

        client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
            try {
                // Kiểm tra nếu bin "personData" tồn tại
                if (!record.bins.containsKey("personData")) {
                    System.out.println("Lỗi: Không tìm thấy bin 'personData' trong record!");
                    return;
                }

                // Lấy dữ liệu protobuf binary từ Aerospike
                byte[] personBinary = (byte[]) record.getValue("personData");

                // Gửi lên Kafka mà không cần giải mã
                ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>(KAFKA_TOPIC, key.userKey.toString(), personBinary);
                producer.send(kafkaRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Lỗi gửi Kafka: " + exception.getMessage());
                    } else {
                        System.out.println("Đã gửi bản ghi có key: " + key.userKey);
                    }
                });

                // Chờ x giây trước khi gửi bản ghi tiếp theo
                // Thread.sleep(3000);

            } catch (Exception e) { 
                e.printStackTrace();
            }
        });
    }
}
