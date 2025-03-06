package com.example;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import com.aerospike.client.Record;


public class AerospikeToKafkaOne {
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "pub";
    private static final String SET_NAME = "users";
    private static final String RECORD_KEY = "b1bd189e-6832-4d77-9600-bf99a7beb1ff"; // Thay bằng PK thực tế

    private static final String KAFKA_BROKER = "0.0.0.0:9092";
    private static final String KAFKA_TOPIC = "person-topic";

    public static void main(String[] args) {
        // Kết nối Aerospike
        AerospikeClient client = new AerospikeClient(new ClientPolicy(), AEROSPIKE_HOST, AEROSPIKE_PORT);

        // Cấu hình Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "0.0.0.0:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        try {
            // Lấy 1 bản ghi duy nhất từ Aerospike
            Key key = new Key(NAMESPACE, SET_NAME, RECORD_KEY);
            Record record = client.get(null, key);

            if (record != null && record.bins.containsKey("personData")) {
                byte[] personData = (byte[]) record.getValue("personData"); // Dữ liệu đã ở dạng protobuf

                // Gửi lên Kafka
                ProducerRecord<String, byte[]> kafkaRecord = new ProducerRecord<>(KAFKA_TOPIC, RECORD_KEY, personData);
                producer.send(kafkaRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Lỗi gửi Kafka: " + exception.getMessage());
                    } else {
                        System.out.println("Đã gửi bản ghi: " + RECORD_KEY);
                    }
                });
            } else {
                System.out.println("Không tìm thấy dữ liệu cho key: " + RECORD_KEY);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
            client.close();
        }
    }
}
