package com.vertx;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AerospikeToKafkaVerticle extends AbstractVerticle {
    private static final String AEROSPIKE_HOST = "127.0.0.1";
    private static final int AEROSPIKE_PORT = 3000;
    private static final String NAMESPACE = "producer";
    private static final String SET_NAME = "users";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String KAFKA_TOPIC = "person-topic";

    private AerospikeClient client;
    private KafkaProducer<String, byte[]> producer;
    private AtomicInteger recordCount = new AtomicInteger(0);
    private static final Logger logger = Logger.getLogger(AerospikeToKafkaVerticle.class.getName());
    private List<KafkaProducerRecord<String, byte[]>> batch = new ArrayList<>();
    private final Queue<KafkaProducerRecord<String, byte[]>> messageQueue = new LinkedList<>();
    private static final int MAX_MESSAGES_PER_SECOND = 5000;
    private boolean isOvertime = false;
    private final AtomicInteger messagesSentThisSecond = new AtomicInteger(0); // Biến đếm số message gửi mỗi giây

    public AerospikeToKafkaVerticle(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        try {
            FileHandler fh = new FileHandler("log/aerospike_to_kafka.log", true);
            fh.setFormatter(new SimpleLogFormatter()); // Sử dụng Formatter tùy chỉnh
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

        // Schedule gửi message từ hàng đợi
        vertx.setPeriodic(1000, id -> {
            synchronized (messageQueue) {
                logger.info("Tổng số message đã gửi trong 1 giây: " + messagesSentThisSecond.get());
                messagesSentThisSecond.set(0); // Đặt lại bộ đếm sau mỗi giây
            }
            sendMessagesFromQueue(); // Chỉ gọi một lần mỗi giây
        });

        readDataFromAero();
        startPromise.complete();
    }

    // Đọc dữ liệu từ Aerospike 
    private void readDataFromAero() {
        vertx.executeBlocking(() -> {
            ScanPolicy scanPolicy = new ScanPolicy();
            scanPolicy.concurrentNodes = true;

            try {
                client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
                    try {
                        if (!record.bins.containsKey("personData")) {
                            logger.warning("Lỗi: Không tìm thấy bin 'personData' trong record!");
                            return;
                        }

                        byte[] personBinary = (byte[]) record.getValue("personData");
                        KafkaProducerRecord<String, byte[]> kafkaRecord = KafkaProducerRecord.create(KAFKA_TOPIC, key.userKey.toString(), personBinary);

                        synchronized (messageQueue) {
                            messageQueue.add(kafkaRecord);
                        }

                        if (messageQueue.size() >= MAX_MESSAGES_PER_SECOND && !isOvertime) {
                            isOvertime = true;
                            vertx.setPeriodic(1000, id -> {
                                if (!messageQueue.isEmpty()) {
                                    sendMessagesFromQueue();
                                } else {
                                    vertx.cancelTimer(id);
                                    isOvertime = false;
                                }
                            });
                        }
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
        }).onSuccess(res -> {
            logger.info("Hoàn thành quét dữ liệu từ Aerospike.");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Đang đóng kết nối...");
                synchronized (batch) {
                    if (!batch.isEmpty()) {
                        logger.info("Gửi batch cuối cùng trước khi đóng...");
                        sendBatch(); // Gửi batch còn lại
                    }
                }
                producer.close();
                client.close();
                try {
                    this.vertx.close();
                } catch (Exception e) {
                    logger.severe("Lỗi khi đóng Vertx: " + e.getMessage());
                }
                logger.info("Đã đóng tất cả kết nối.");
            }));
        }).onFailure(err -> {
            logger.severe("Lỗi khi quét dữ liệu từ Aerospike: " + err.getMessage());
        });
    }

    // Gửi batch Kafka
    private void sendBatch() {
        List<KafkaProducerRecord<String, byte[]>> batchToSend;
        synchronized (batch) {
            batchToSend = new ArrayList<>(batch);
            batch.clear();
        }
        messageQueue.addAll(batchToSend);

        if (messageQueue.size() >= 5000 && !isOvertime) {
            isOvertime = true;
            sendMessagesFromQueue();
        } else {
            vertx.setTimer(1000, id -> {
                if (!messageQueue.isEmpty() && !isOvertime) {
                    isOvertime = true;
                    logger.info("Timeout: Gửi batch còn lại trong hàng đợi.");
                    sendMessagesFromQueue();
                }
            });
        }
    }

    // Gửi lại message nếu thất bại
    private void retrySend(KafkaProducerRecord<String, byte[]> record) {
        int maxRetries = 3;
        AtomicInteger retryCount = new AtomicInteger(0);

        producer.send(record, result -> {
            if (result.failed() && retryCount.incrementAndGet() <= maxRetries) {
                logger.warning("Retry lần " + retryCount.get() + " cho record: " + result.cause().getMessage());
                retrySend(record); // Retry logic
            } else if (result.failed()) {
                logger.severe("Retry thất bại sau " + maxRetries + " lần: " + result.cause().getMessage());
            } else {
                recordCount.incrementAndGet();
            }
        });
    }

    // Gửi message từ hàng đợi len Kafka
    private void sendMessagesFromQueue() {
        int count = 0;
        List<KafkaProducerRecord<String, byte[]>> batchToSend = new ArrayList<>();

        synchronized (messageQueue) {
            while (count < MAX_MESSAGES_PER_SECOND && !messageQueue.isEmpty()) {
                batchToSend.add(messageQueue.poll());
                count++;
            }
        }

        for (KafkaProducerRecord<String, byte[]> record : batchToSend) {
            producer.send(record, result -> {
                if (result.failed()) {
                    logger.severe("Lỗi gửi Kafka: " + result.cause().getMessage());
                    retrySend(record);
                } else {
                    recordCount.incrementAndGet();
                    messagesSentThisSecond.incrementAndGet(); // Tăng bộ đếm mỗi khi gửi thành công
                }
            });
        }

        logger.info("Đã gửi " + count + " message trong 1 giây.");
        isOvertime = false; // Đặt lại cờ sau khi gửi xong
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
