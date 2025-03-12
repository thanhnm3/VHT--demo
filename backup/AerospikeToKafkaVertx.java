package com.vertx;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
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

public class AerospikeToKafkaVertx {
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "pub";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";

    private static AerospikeClient client;
    private static KafkaProducer<String, byte[]> producer;
    private static Vertx vertx;
    private static AtomicInteger recordCount = new AtomicInteger(0);
    private static final Logger logger = Logger.getLogger(AerospikeToKafkaVertx.class.getName());

    public static void main(String[] args) {
        try {
            FileHandler fh = new FileHandler("aerospike_to_kafka.log", true);
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Kết nối Aerospike
        client = new AerospikeClient(new ClientPolicy(), AEROSPIKE_HOST, AEROSPIKE_PORT);
        
        // Cấu hình Vert.x và Kafka Producer
        vertx = Vertx.vertx();
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", KAFKA_BROKER);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = KafkaProducer.create(vertx, config);

        // Scheduled task để log số bản ghi gửi mỗi giây
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            int count = recordCount.getAndSet(0);
            logger.info("Đã gửi " + count + " bản ghi trong giây vừa qua.");
        }, 1, 1, TimeUnit.SECONDS);

        // Gửi dữ liệu lên Kafka
        sendDataToKafka();
    }

    private static void sendDataToKafka() {
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.concurrentNodes = true;

        client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
            try {
                if (!record.bins.containsKey("personData")) {
                    logger.warning("Lỗi: Không tìm thấy bin 'personData' trong record!");
                    return;
                }

                byte[] personBinary = (byte[]) record.getValue("personData");
                KafkaProducerRecord<String, byte[]> kafkaRecord = KafkaProducerRecord.create(KAFKA_TOPIC, key.userKey.toString(), personBinary);
                
                producer.send(kafkaRecord, result -> {
                    if (result.failed()) {
                        logger.severe("Lỗi gửi Kafka: " + result.cause().getMessage());
                    } else {
                        recordCount.incrementAndGet();
                    }
                });

            } catch (Exception e) {
                logger.severe("Lỗi xử lý record: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Đợi cho đến khi tất cả bản ghi được gửi xong
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Đang đóng kết nối...");
            producer.close();
            client.close();
            vertx.close();
            logger.info("Đã đóng tất cả kết nối.");
        }));
    }
}
