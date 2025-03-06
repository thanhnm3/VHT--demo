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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaToAerospikeThread {

    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "sub";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final String GROUP_ID = "aerospike-consumer-group";
    private static final int NUM_THREADS = 2;

    public static void main(String[] args) {
        // Initialize Aerospike client
        AerospikeClient aerospikeClient = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);
        WritePolicy writePolicy = new WritePolicy();

        // Initialize Kafka consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create a thread pool for consumers
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorService.submit(new ConsumerTask(props, aerospikeClient, writePolicy));
        }

        // Shutdown the executor service after 50 seconds
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(50, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }

        // Close Aerospike client
        aerospikeClient.close();
    }

    static class ConsumerTask implements Runnable {
        private final Properties props;
        private final AerospikeClient aerospikeClient;
        private final WritePolicy writePolicy;

        ConsumerTask(Properties props, AerospikeClient aerospikeClient, WritePolicy writePolicy) {
            this.props = props;
            this.aerospikeClient = aerospikeClient;
            this.writePolicy = writePolicy;
        }

        @Override
        public void run() {
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

                while (true) {
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
            }
        }
    }
}

// kafka-producer-perf-test --topic your_topic --num-records 1000000 --record-size 1000 --throughput -1 --producer.config config.properties
// docker exec -it 5fac09b6af654f88d6f2ff27cb6fb306e1bf326f554263e7837043deddad5a48 /bin/sh