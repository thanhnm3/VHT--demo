package com.vertx;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.text.SimpleDateFormat;
import java.util.Date;

public class KafkaToAerospikeVerticle extends AbstractVerticle {

    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 4000;
    private static final String NAMESPACE = "consumer";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final String GROUP_ID = "aerospike-consumer-group";
    // private static final int MAX_MESSAGES_PER_SECOND = 5000;  // Giới hạn số lượng message mỗi giây

    private AerospikeClient aerospikeClient;
    private KafkaConsumer<byte[], byte[]> consumer;
    private AtomicInteger insertCount = new AtomicInteger(0);
    private static final Logger logger = Logger.getLogger(KafkaToAerospikeVerticle.class.getName());
    private List<KafkaConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
    private final AtomicInteger messagesSentThisSecond = new AtomicInteger(0); // Biến đếm số message gửi mỗi giây

    public KafkaToAerospikeVerticle(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        try {
            FileHandler fh = new FileHandler("log/kafka_to_aerospike.log", true);
            fh.setFormatter(new SimpleLogFormatter());
            logger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("🔄 KafkaToAerospikeVerticle sử dụng Vertx với Worker Pool Size: " + (Runtime.getRuntime().availableProcessors() * 4));

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
        config.put("enable.auto.commit", "false");
        config.put("max.poll.interval.ms", "300000");
        consumer = KafkaConsumer.create(this.vertx, config);
        consumer.subscribe(KAFKA_TOPIC);

        // Schedule to send batch every second
        vertx.setPeriodic(1000, id -> {
            sendBatch(writePolicy);
            logger.info("So message da gui trong 1 giay : " + messagesSentThisSecond.get());
            messagesSentThisSecond.set(0); // Đặt lại bộ đếm sau mỗi giây
        });

        // Consume messages from Kafka and add to batch
        consumer.handler(record -> {
            synchronized (batch) {
                batch.add(record);
            }
        });

        startPromise.complete();
    }

    private void sendBatch(WritePolicy writePolicy) {
        List<KafkaConsumerRecord<byte[], byte[]>> batchToSend;
        synchronized (batch) {
            batchToSend = new ArrayList<>(batch);
            batch.clear();
        }

        for (KafkaConsumerRecord<byte[], byte[]> record : batchToSend) {
            try {
                byte[] keyBytes = record.key();
                byte[] value = record.value();

                if (keyBytes == null || value == null) {
                    logger.warning("Received null key or value, skipping record.");
                    continue;
                }

                // Convert key to string
                String keyString = new String(keyBytes);

                // Create Aerospike key and bins
                Key aerospikeKey = new Key(NAMESPACE, SET_NAME, keyString);
                Bin dataBin = new Bin("data", value);
                Bin PKBin = new Bin("PK", keyString);

                // Write to Aerospike
                aerospikeClient.put(writePolicy, aerospikeKey, PKBin, dataBin);
                insertCount.incrementAndGet();
                messagesSentThisSecond.incrementAndGet(); // Tăng bộ đếm mỗi khi gửi thành công
            } catch (Exception e) {
                logger.severe("Failed to process record: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Commit offset after processing batch
        if (!batchToSend.isEmpty()) {
            consumer.commit(ar -> {
                if (ar.succeeded()) {
                    logger.info("Offset committed successfully.");
                } else {
                    logger.severe("Failed to commit offset: " + ar.cause().getMessage());
                }
            });
        }
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

// Formatter log tùy chỉnh de don gian hon 
class SimpleLogFormatter extends Formatter {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String format(LogRecord record) {
        String timestamp = dateFormat.format(new Date(record.getMillis()));
        return String.format("%s: %s%n", timestamp, record.getMessage());
    }
}