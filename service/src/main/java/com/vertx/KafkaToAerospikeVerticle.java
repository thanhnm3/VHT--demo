package com.vertx;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class KafkaToAerospikeVerticle extends AbstractVerticle {

    // Cấu hình kết nối Aerospike
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 4000;
    private static final String NAMESPACE = "consumer";
    private static final String SET_NAME = "users";

    // Cấu hình Kafka
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";
    private static final String GROUP_ID = "aerospike-consumer-group";
    private static final int MAX_MESSAGES_PER_BATCH = 5000; // Giới hạn số lượng message mỗi batch


    private AerospikeClient aerospikeClient; // Kết nối Aerospike
    private KafkaConsumer<byte[], byte[]> consumer; // Kafka consumer
    private AtomicInteger insertCount = new AtomicInteger(0); // Đếm số bản ghi đã chèn thành công
    private static final Logger logger = Logger.getLogger(KafkaToAerospikeVerticle.class.getName()); // Logger
    private final Queue<KafkaConsumerRecord<byte[], byte[]>> messageQueue = new ConcurrentLinkedQueue<>(); // Hàng đợi lưu trữ message từ Kafka


    private final AtomicInteger messagesProcessedThisSecond = new AtomicInteger(0); // Đếm số message xử lý trong 1 giây

    @Override
    public void start(Promise<Void> startPromise) {
        try {
            // Cấu hình logger ghi log vào file
            FileHandler fh = new FileHandler("log/kafka_to_aerospike.log", true);
            fh.setFormatter(new SimpleLogFormatter());
            logger.addHandler(fh);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("🔄 KafkaToAerospikeVerticle sử dụng Vertx với Worker Pool Size: " + (Runtime.getRuntime().availableProcessors() * 4));

        // Khởi tạo kết nối Aerospike
        aerospikeClient = new AerospikeClient(AEROSPIKE_HOST, AEROSPIKE_PORT);
        WritePolicy writePolicy = new WritePolicy();

        // Khởi tạo Kafka consumer
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", KAFKA_BROKER); // Địa chỉ Kafka broker
        config.put("group.id", GROUP_ID); // Group ID của consumer
        config.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"); // Deserializer cho key
        config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"); // Deserializer cho value
        config.put("auto.offset.reset", "earliest"); // Đọc từ offset sớm nhất
        config.put("enable.auto.commit", "false"); // Tắt auto commit offset
        config.put("max.poll.interval.ms", "300000"); // Thời gian tối đa giữa các lần poll
        consumer = KafkaConsumer.create(this.vertx, config);
        consumer.subscribe(KAFKA_TOPIC); // Đăng ký topic Kafka

        // Lên lịch xử lý và gửi message mỗi giây
        vertx.setPeriodic(1000, id -> {
            processAndSendBatch(writePolicy); // Xử lý và gửi batch vào Aerospike
            logger.info("Số message đã xử lý trong 1 giây: " + messagesProcessedThisSecond.get());
            messagesProcessedThisSecond.set(0); // Reset bộ đếm mỗi giây
        });

        // Xử lý message từ Kafka và thêm vào hàng đợi
        consumer.handler(record -> {
            messageQueue.add(record); // Thêm message vào hàng đợi mà không cần synchronized
        });

        startPromise.complete(); // Hoàn thành khởi động Verticle
    }

    // Hàm xử lý và gửi batch vào Aerospike
    private void processAndSendBatch(WritePolicy writePolicy) {
        List<KafkaConsumerRecord<byte[], byte[]>> batchToSend = new ArrayList<>();

        // Lấy tối đa 5000 bản ghi từ hàng đợi
        while (!messageQueue.isEmpty() && batchToSend.size() < MAX_MESSAGES_PER_BATCH) {
            KafkaConsumerRecord<byte[], byte[]> record = messageQueue.poll();
            if (record != null) {
                batchToSend.add(record);
            }
        }

        if (batchToSend.isEmpty()) {
            return; // Không có bản ghi nào để xử lý
        }

        for (KafkaConsumerRecord<byte[], byte[]> record : batchToSend) {
            try {
                byte[] keyBytes = record.key();
                byte[] value = record.value();

                if (keyBytes == null || value == null) {
                    logger.warning("Received null key or value, skipping record.");
                    continue;
                }

                String keyString = new String(keyBytes);
                Key aerospikeKey = new Key(NAMESPACE, SET_NAME, keyString);
                Bin dataBin = new Bin("data", value);
                Bin PKBin = new Bin("PK", keyString);

                // Ghi dữ liệu vào Aerospike
                aerospikeClient.put(writePolicy, aerospikeKey, PKBin, dataBin);
                insertCount.incrementAndGet();
                messagesProcessedThisSecond.incrementAndGet();
            } catch (Exception e) {
                logger.severe("Failed to process record: " + e.getMessage());
            }
        }

        // Commit offset Kafka sau khi xử lý xong batch
        consumer.commit(commitRes -> {
            if (commitRes.succeeded()) {
                logger.info("✅ Batch committed successfully: " + batchToSend.size() + " records");
            } else {
                logger.severe("❌ Failed to commit Kafka offsets: " + commitRes.cause().getMessage());
            }
        });
    }

    @Override
    public void stop() {
        // Đóng Kafka consumer
        if (consumer != null) {
            consumer.close();
            logger.info("Kafka Consumer closed.");
        }
        // Đóng Aerospike client
        if (aerospikeClient != null) {
            aerospikeClient.close();
            logger.info("Aerospike Client closed.");
        }
    }
}

