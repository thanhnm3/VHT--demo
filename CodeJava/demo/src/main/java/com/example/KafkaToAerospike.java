package com.example;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaToAerospike {
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "sub";
    private static final String SET_NAME = "users";

    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final String GROUP_ID = "aerospike-consumer-group";

    public static void main(String[] args) {
        // Kết nối Aerospike
        AerospikeClient client = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);
        WritePolicy writePolicy = new WritePolicy();

        // Cấu hình Kafka Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

        System.out.println("Bắt đầu lắng nghe từ Kafka...");

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> {
            System.out.println("Dừng lắng nghe từ Kafka sau 10 giây.");
            consumer.wakeup();
        }, 10, TimeUnit.SECONDS);

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, byte[]> record : records) {
                    String userKey = record.key();
                    byte[] personBinary = record.value();

                    // Lưu vào Aerospike
                    Key key = new Key(NAMESPACE, SET_NAME, userKey);
                    Bin bin = new Bin("personData", personBinary);
                    client.put(writePolicy, key, bin);

                    System.out.println("Đã lưu vào Aerospike: Key = " + userKey);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            client.close();
            scheduler.shutdown();
        }
    }
}
