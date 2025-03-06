package com.example;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AerospikeToKafkaBatch {
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "pub";
    private static final String SET_NAME = "users";

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final int BATCH_SIZE = 10; // Batch size

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

        // 3. Gửi dữ liệu lên Kafka và dừng chương trình khi hoàn tất
        sendDataToKafka();

        // Đóng kết nối sau khi gửi xong
        System.out.println("Đã gửi xong tất cả dữ liệu. Đóng kết nối...");
        producer.close();
        client.close();
    }

    private static void sendDataToKafka() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;

        List<ProducerRecord<String, byte[]>> buffer = new ArrayList<>();

        client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
            try {
                // Kiểm tra nếu bin "personData" tồn tại
                if (!record.bins.containsKey("personData")) {
                    System.out.println("Lỗi: Không tìm thấy bin 'personData' trong record!");
                    return;
                }

                // Lấy dữ liệu protobuf binary từ Aerospike
                byte[] personBinary = (byte[]) record.getValue("personData");

                // Thêm vào buffer
                ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>(KAFKA_TOPIC, key.userKey.toString(), personBinary);
                buffer.add(kafkaRecord);

                // Kiểm tra nếu buffer đã đạt đến kích thước batch
                if (buffer.size() >= BATCH_SIZE) {
                    processBatch(buffer);
                    buffer.clear();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Gửi các bản ghi còn lại trong buffer
        if (!buffer.isEmpty()) {
            processBatch(buffer);
        }

        System.out.println("Đã gửi xong tất cả dữ liệu.");
    }

    private static void processBatch(List<ProducerRecord<String, byte[]>> buffer) {
        for (ProducerRecord<String, byte[]> record : buffer) {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Lỗi gửi Kafka: " + exception.getMessage());
                } else {
                    System.out.println("Đã gửi bản ghi có key: " + record.key());
                }
            });
        }
        // Đợi cho đến khi tất cả bản ghi trong batch được gửi xong
        producer.flush();
    }
}