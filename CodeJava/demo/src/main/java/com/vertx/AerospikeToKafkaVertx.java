package com.vertx;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

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

public class AerospikeToKafkaVertx extends AbstractVerticle {

    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "pub";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";

    private AerospikeClient aerospikeClient;
    private KafkaProducer<byte[], byte[]> producer;
    private AtomicInteger insertCount = new AtomicInteger(0);
    private static final Logger logger = Logger.getLogger(AerospikeToKafkaVertx.class.getName());

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new AerospikeToKafkaVertx());
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
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = true;

        // Initialize Kafka producer
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", KAFKA_BROKER);
        config.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        config.put("acks", "1");

        producer = KafkaProducer.create(vertx, config);

        // Scheduled task to log insert count every second
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            int count = insertCount.getAndSet(0);
            logger.info("Sent " + count + " records to Kafka in the last second.");
        }, 1, 1, TimeUnit.SECONDS);

        // Scan all records from Aerospike and send them to Kafka
        scanAerospike(scanPolicy);
    }

    private void scanAerospike(ScanPolicy scanPolicy) {
        logger.info("Starting Aerospike scan...");
        aerospikeClient.scanAll(scanPolicy, NAMESPACE, SET_NAME, new ScanCallback() {
            @Override
            public void scanCallback(Key key, Record record) {
                processRecord(key, record);
            }
        });
        logger.info("Completed scanning all records from Aerospike.");
    }

    private void processRecord(Key key, Record record) {
        if (key == null || record == null) {
            logger.warning("Skipping record due to null key or record.");
            return;
        }

        // Xử lý key
        Object userKey = key.userKey.getObject();
        byte[] keyBytes;
        if (userKey instanceof String) {
            keyBytes = ((String) userKey).getBytes();
        } else if (userKey instanceof byte[]) {
            keyBytes = (byte[]) userKey;
        } else {
            logger.warning("Unknown key type: " + userKey);
            return;
        }

        // Xử lý value
        Object rawValue = record.getValue("data");
        byte[] value;
        if (rawValue instanceof byte[]) {
            value = (byte[]) rawValue;
        } else if (rawValue instanceof String) {
            value = ((String) rawValue).getBytes();
        } else {
            logger.warning("Skipping record with unexpected value type: " + rawValue);
            return;
        }

        // Log để kiểm tra trước khi gửi Kafka
        logger.info("Sending to Kafka - Key: " + new String(keyBytes) + ", Value Length: " + value.length);

        // Tạo Kafka producer record
        KafkaProducerRecord<byte[], byte[]> kafkaRecord = KafkaProducerRecord.create(KAFKA_TOPIC, keyBytes, value);

        // Gửi dữ liệu lên Kafka
        producer.send(kafkaRecord, ar -> {
            if (ar.succeeded()) {
                insertCount.incrementAndGet();
            } else {
                logger.severe("Failed to send record to Kafka: " + ar.cause().getMessage());
            }
        });
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
