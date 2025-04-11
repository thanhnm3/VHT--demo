package com.vertx;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class AerospikeToKafkaVerticle extends AbstractVerticle {
    // Cấu hình Aerospike
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "producer";
    private static final String SET_NAME = "users";

    // Cấu hình Kafka
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";

    private AerospikeClient client; // Kết nối Aerospike
    private KafkaProducer<String, byte[]> producer; // Kafka producer
    private AtomicInteger recordCount = new AtomicInteger(0); // Đếm số bản ghi đã gửi thành công
    private static final Logger logger = Logger.getLogger(AerospikeToKafkaVerticle.class.getName()); // Logger

    private final Queue<KafkaProducerRecord<String, byte[]>> messageQueue = new ConcurrentLinkedQueue<>(); // Hàng đợi message
    private static final int MAX_MESSAGES_PER_SECOND = 5000; // Giới hạn số message gửi mỗi giây
    private final AtomicInteger messagesSentThisSecond = new AtomicInteger(0); // Đếm số message gửi trong 1 giây

    @Override
    public void start(Promise<Void> startPromise) {
        try {
            // Cấu hình logger ghi log vào file
            FileHandler fh = new FileHandler("log/aerospike_to_kafka.log", true);
            fh.setFormatter(new SimpleLogFormatter()); // Sử dụng formatter tùy chỉnh
            if (logger.getHandlers().length == 0) {
                logger.addHandler(fh);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Kết nối Aerospike
        client = new AerospikeClient(new ClientPolicy(), AEROSPIKE_HOST, AEROSPIKE_PORT);

        // Kết nối Kafka
        logger.info("🔄 KafkaToAerospikeVerticle sử dụng Vertx với Worker Pool Size: " + (Runtime.getRuntime().availableProcessors() * 2));
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", KAFKA_BROKER);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = KafkaProducer.create(this.vertx, config);

        // Đọc dữ liệu từ Aerospike
        readDataFromAero();

        // Bắt đầu xử lý và gửi message
        processAndSendMessages();

        startPromise.complete(); // Hoàn thành khởi động Verticle
    }

    // Đọc dữ liệu từ Aerospike
    private void readDataFromAero() {
        vertx.executeBlocking(() -> {
            ScanPolicy scanPolicy = new ScanPolicy(); // Chính sách quét Aerospike
            scanPolicy.concurrentNodes = true; // Cho phép quét đồng thời trên các node

            try {
                // Quét tất cả các bản ghi trong set
                client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
                    try {
                        // Kiểm tra bin 'personData' có tồn tại không
                        if (!record.bins.containsKey("personData")) {
                            logger.warning("Lỗi: Không tìm thấy bin 'personData' trong record!");
                            return;
                        }

                        // Tạo Kafka record từ dữ liệu Aerospike
                        byte[] personBinary = (byte[]) record.getValue("personData");
                        KafkaProducerRecord<String, byte[]> kafkaRecord = KafkaProducerRecord.create(KAFKA_TOPIC, key.userKey.toString(), personBinary);

                        // Gửi trực tiếp đến Kafka
                        producer.send(kafkaRecord, result -> {
                            if (result.failed()) {
                                logger.severe("Lỗi gửi Kafka: " + result.cause().getMessage());
                                retrySend(kafkaRecord); // Retry nếu gửi thất bại
                            } else {
                                recordCount.incrementAndGet();
                                messagesSentThisSecond.incrementAndGet();
                            }
                        });
                    } catch (Exception e) {
                        logger.severe("Lỗi xử lý record: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                logger.severe("Lỗi khi quét dữ liệu từ Aerospike: " + e.getMessage());
                throw e; // Ném lỗi để Vert.x xử lý
            }
            return null;
        }).onSuccess(res -> logger.info("Hoàn thành quét dữ liệu từ Aerospike."))
          .onFailure(err -> logger.severe("Lỗi khi quét dữ liệu từ Aerospike: " + err.getMessage()));
    }

    // Gửi message từ hàng đợi lên Kafka mỗi giây
    private void processAndSendMessages() {
        vertx.setPeriodic(1000, id -> {
            List<KafkaProducerRecord<String, byte[]>> batchToSend = new ArrayList<>();

            // Lấy tối đa 5000 bản ghi từ hàng đợi
            while (!messageQueue.isEmpty() && batchToSend.size() < MAX_MESSAGES_PER_SECOND) {
                batchToSend.add(messageQueue.poll());
            }

            if (!batchToSend.isEmpty()) {
                for (KafkaProducerRecord<String, byte[]> record : batchToSend) {
                    producer.send(record, result -> {
                        if (result.failed()) {
                            logger.severe("Lỗi gửi Kafka: " + result.cause().getMessage());
                            retrySend(record);
                        } else {
                            recordCount.incrementAndGet();
                        }
                    });
                    // Tăng biến đếm ngay khi gửi
                    messagesSentThisSecond.incrementAndGet();
                }
                logger.info("Đã gửi " + batchToSend.size() + " message. Hàng đợi hiện tại: " + messageQueue.size());
            } else {
                logger.info("Queue rỗng tại thời điểm gửi. Hàng đợi hiện tại: " + messageQueue.size());
            }

            // Reset bộ đếm messagesSentThisSecond về 0 mỗi giây
            logger.info("Số message đã gửi trong 1 giây: " + messagesSentThisSecond.get());
            messagesSentThisSecond.set(0);
        });
    }

    // Gửi lại message nếu thất bại
    private void retrySend(KafkaProducerRecord<String, byte[]> record) {
        int maxRetries = 3; // Số lần retry tối đa
        AtomicInteger retryCount = new AtomicInteger(0); // Đếm số lần retry

        producer.send(record, result -> {
            if (result.failed() && retryCount.incrementAndGet() <= maxRetries) {
                logger.warning("Retry lần " + retryCount.get() + " cho record: " + result.cause().getMessage());
                retrySend(record); // Retry logic
            } else if (result.failed()) {
                logger.severe("Retry thất bại sau " + maxRetries);
            } else {
                recordCount.incrementAndGet(); // Tăng bộ đếm nếu gửi thành công
            }
        });
    }
}


