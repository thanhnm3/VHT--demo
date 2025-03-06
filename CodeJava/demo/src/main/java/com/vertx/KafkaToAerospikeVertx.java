package com.vertx;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class KafkaToAerospikeVertx extends AbstractVerticle {

    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "sub";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final String GROUP_ID = "aerospike-consumer-group";

    private AerospikeClient aerospikeClient;
    private KafkaConsumer<byte[], byte[]> consumer;
    private AtomicInteger insertCount = new AtomicInteger(0);
    private static final Logger logger = Logger.getLogger(KafkaToAerospikeVertx.class.getName());

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new KafkaToAerospikeVertx());
    }

    @Override
    public void start() {
        try {
            FileHandler fh = new FileHandler("kafka_to_aerospike.log", true);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Initialize Aerospike client
        aerospikeClient = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);
        WritePolicy writePolicy = new WritePolicy();

        // Initialize Kafka consumer
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", KAFKA_BROKER);
        config.put("group.id", GROUP_ID);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        config.put("auto.offset.reset", "earliest");

        consumer = KafkaConsumer.create(vertx, config);
        consumer.subscribe(KAFKA_TOPIC);

        // Scheduled task to log insert count every second
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            int count = insertCount.getAndSet(0);
            logger.info("Inserted " + count + " records in the last second.");
        }, 1, 1, TimeUnit.SECONDS);

        // Consume messages from Kafka and store them in Aerospike
        consumer.handler(record -> {
            try {
                byte[] key = record.key();
                byte[] value = record.value();

                if (key == null || value == null) {
                    logger.warning("Received null key or value, skipping record.");
                    return;
                }

                // Create Aerospike key and bins
                Key aerospikeKey = new Key(NAMESPACE, SET_NAME, key);
                Bin bin = new Bin("data", value);

                // Write to Aerospike
                aerospikeClient.put(writePolicy, aerospikeKey, bin);
                insertCount.incrementAndGet();
            } catch (Exception e) {
                logger.severe("Failed to process record: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    @Override
    public void stop() {
        if (consumer != null) {
            consumer.close();
            logger.info("Kafka Consumer closed.");
        }
        if (aerospikeClient != null) {
            aerospikeClient.close();
            logger.info("Aerospike Client closed.");
        }
    }

}
