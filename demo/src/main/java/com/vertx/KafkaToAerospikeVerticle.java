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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class KafkaToAerospikeVerticle extends AbstractVerticle {

    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 4000;
    private static final String NAMESPACE = "consumer";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final String GROUP_ID = "aerospike-consumer-group";
    private static final int BATCH_SIZE = 10;  // Kích thước batch

    private AerospikeClient aerospikeClient;
    private KafkaConsumer<byte[], byte[]> consumer;
    private AtomicInteger insertCount = new AtomicInteger(0);
    private static final Logger logger = Logger.getLogger(KafkaToAerospikeVerticle.class.getName());
    private List<KafkaConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();

    public KafkaToAerospikeVerticle(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        try {
            FileHandler fh = new FileHandler("log/kafka_to_aerospike.log", true);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Tạo một instance Vertx riêng với cấu hình worker pool khác
        // Vertx vertx2 = Vertx.vertx(new VertxOptions().setWorkerPoolSize(Runtime.getRuntime().availableProcessors() * 4));
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

        // Tối ưu Consumer  
        config.put("enable.auto.commit", "false"); // Không auto commit, tránh mất dữ liệu  
        // config.put("max.poll.records", "6000"); // Giới hạn số message mỗi lần poll  
        // config.put("fetch.max.bytes", "10485760"); // Giới hạn kích thước message  
        config.put("max.poll.interval.ms", "300000"); // Tránh bị Kafka kick nếu xử lý chậm  

        consumer = KafkaConsumer.create(this.vertx, config); // Sử dụng vertx2 cho Kafka consumer
        consumer.subscribe(KAFKA_TOPIC);

        // Scheduled task to log insert count every second
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            int count = insertCount.getAndSet(0);
            logger.info("Kafka ----------> " + count + " records ----------> Aerospike");
        }, 1, 1, TimeUnit.SECONDS);

        // Consume messages from Kafka and store them in Aerospike
        consumer.handler(record -> {
            synchronized (batch) {
                batch.add(record);
                if (batch.size() >= BATCH_SIZE) {
                    sendBatch(writePolicy);
                }
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
            } catch (Exception e) {
                logger.severe("Failed to process record: " + e.getMessage());
                e.printStackTrace();
            }
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