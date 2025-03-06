package com.vertx;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaToAerospike {

    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "sub";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final String GROUP_ID = "aerospike-consumer-group";

    public static void main(String[] args) {
        // Initialize Aerospike client
        AerospikeClient aerospikeClient = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);
        WritePolicy writePolicy = new WritePolicy();

        // Initialize Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

        // Consume messages from Kafka and store them in Aerospike
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                byte[] key = record.key();
                byte[] value = record.value();

                // Create Aerospike key and bins
                Key aerospikeKey = new Key(NAMESPACE, SET_NAME, key);
                Bin bin = new Bin("data", value);

                // Write to Aerospike
                aerospikeClient.put(writePolicy, aerospikeKey, bin);
            }
        }

        // Close Aerospike client
        aerospikeClient.close();
        // Close Kafka consumer
        consumer.close();
    }
}