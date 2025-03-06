package com.vertx;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class KafkaFromAerospikeVertx extends AbstractVerticle {

    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "pub";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";

    private AerospikeClient aerospikeClient;
    private KafkaProducer<String, byte[]> producer;
    private static final Logger logger = Logger.getLogger(KafkaFromAerospikeVertx.class.getName());

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new KafkaFromAerospikeVertx());
    }

    @Override
    public void start() {
        try {
            FileHandler fh = new FileHandler("aerospike_to_kafka.log", true);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Initialize Aerospike client
        aerospikeClient = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);

        // Initialize Kafka producer
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", KAFKA_BROKER);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("acks", "1");

        producer = KafkaProducer.create(vertx, config);

        // Scan Aerospike for records
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;
        scanPolicy.includeBinData = true;

        try {
            aerospikeClient.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
                try {
                    String recordKey = key.userKey.toString();
                    byte[] recordValue = (byte[]) record.getValue("personData");

                    if (recordKey == null || recordValue == null) {
                        logger.warning("Null key or value, skipping record.");
                        return;
                    }

                    // Create Kafka producer record
                    KafkaProducerRecord<String, byte[]> kafkaRecord = KafkaProducerRecord.create(KAFKA_TOPIC, recordKey, recordValue);

                    // Send to Kafka
                    producer.send(kafkaRecord, ar -> {
                        if (ar.succeeded()) {
                            logger.info("Sent record to Kafka: " + recordKey);
                        } else {
                            logger.severe("Failed to send record to Kafka: " + ar.cause().getMessage());
                        }
                    });
                } catch (Exception e) {
                    logger.severe("Failed to process record: " + e.getMessage());
                }
            });
        } catch (AerospikeException e) {
            logger.severe("Aerospike scan failed: " + e.getMessage());
        }
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
            logger.info("Kafka Producer closed.");
        }
        if (aerospikeClient != null) {
            aerospikeClient.close();
            logger.info("Aerospike Client closed.");
        }
    }

}