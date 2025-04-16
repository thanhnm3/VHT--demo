package com.cdc;

import com.aerospike.client.*;
import com.aerospike.client.query.*;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.*;
import com.aerospike.client.query.Filter;
import com.aerospike.client.Record;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CdcProducer {

    // Load configuration from .env
    private static final Dotenv dotenv = Dotenv.configure().directory("service//.env").load();

    // Aerospike configuration
    private static final String AERO_HOST = dotenv.get("AEROSPIKE_PRODUCER_HOST");
    private static final int AERO_PORT = Integer.parseInt(dotenv.get("AEROSPIKE_PRODUCER_PORT"));
    private static final String NAMESPACE = dotenv.get("PRODUCER_NAMESPACE");
    private static final String SET_NAME = dotenv.get("PRODUCER_SET_NAME");
    private static final String BIN_NAME = "personData";
    private static final String BIN_LAST_UPDATE = "last_update";

    // Kafka configuration
    private static final String KAFKA_BROKER = dotenv.get("KAFKA_BROKER");
    private static final String KAFKA_TOPIC = dotenv.get("KAFKA_TOPIC_CDC");
    private static final int MAX_RETRIES = Integer.parseInt(dotenv.get("MAX_RETRIES"));

    private static long lastPolledTime = System.currentTimeMillis() - 10_000; // Bắt đầu từ 10s trước
    private static final AtomicInteger messagesSentThisSecond = new AtomicInteger(0); // Đếm số lượng message gửi mỗi giây

    public static void main(String[] args, int threadPoolSize) {
        AerospikeClient client = null;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(threadPoolSize); // Sử dụng threadPoolSize
        try {
            client = new AerospikeClient(AERO_HOST, AERO_PORT);
            Producer<String, byte[]> producer = createKafkaProducer();

            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages sent : " + messagesSentThisSecond.get());
                messagesSentThisSecond.set(0); // Reset bộ đếm
            }, 0, 1, TimeUnit.SECONDS);

            while (true) {
                try {
                    Statement stmt = new Statement();
                    stmt.setNamespace(NAMESPACE);
                    stmt.setSetName(SET_NAME);

                    // Sử dụng Filter để lọc các bản ghi dựa trên giá trị của BIN_LAST_UPDATE
                    stmt.setFilter(Filter.range(BIN_LAST_UPDATE, lastPolledTime + 1, System.currentTimeMillis()));

                    RecordSet records = client.query(null, stmt);

                    try {
                        while (records.next()) {
                            Key key = records.getKey();
                            Record record = records.getRecord();

                            byte[] data = record.getBytes(BIN_NAME);
                            if (data != null) {
                                // Tạo message ở dạng JSON và mã hóa UTF-8
                                String message = String.format("{\"personData\": \"%s\", \"lastUpdate\": %d}",
                                    Base64.getEncoder().encodeToString(data), record.getLong(BIN_LAST_UPDATE));
                                sendWithRetry(producer, new ProducerRecord<>(KAFKA_TOPIC, key.userKey.toString(), message.getBytes(StandardCharsets.UTF_8)), 0);
                                messagesSentThisSecond.incrementAndGet(); // Tăng bộ đếm
                            }

                            long updateTime = record.getLong(BIN_LAST_UPDATE);
                            if (updateTime > lastPolledTime) {
                                lastPolledTime = updateTime;
                            }
                        }
                    } finally {
                        records.close(); // Đảm bảo đóng RecordSet
                    }

                    Thread.sleep(1000); // Poll mỗi 1 giây
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (AerospikeException e) {
            System.err.println("Failed to connect to Aerospike: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close(); // Đảm bảo đóng client
                System.out.println("Aerospike client closed.");
            }
            scheduler.shutdown(); // Đảm bảo đóng scheduler
        }
    }

    private static Producer<String, byte[]> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("linger.ms", "5"); // Thời gian chờ để gom batch
        props.put("batch.size", "32768"); // Kích thước batch tối đa (32KB)
        props.put("acks", "all"); // Đảm bảo tất cả các bản sao nhận được dữ liệu
        return new KafkaProducer<>(props);
    }

    private static void sendWithRetry(Producer<String, byte[]> producer, ProducerRecord<String, byte[]> record, int retryCount) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                if (retryCount < MAX_RETRIES) {
                    System.err.println("Retrying message. Attempt: " + (retryCount + 1));
                    sendWithRetry(producer, record, retryCount + 1);
                } else {
                    System.err.println("Failed to send message after " + MAX_RETRIES + " retries: " + exception.getMessage());
                }
            }
        });
    }
}

