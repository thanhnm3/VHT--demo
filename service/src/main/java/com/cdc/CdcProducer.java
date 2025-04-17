package com.cdc;

import com.aerospike.client.*;
import com.aerospike.client.query.*;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.*;
import com.aerospike.client.Record;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutorService;
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
        // Tạo thread pool chỉ cho producer (gửi dữ liệu)
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

        // Scheduler chỉ dùng 1 thread để in log mỗi giây
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            client = new AerospikeClient(AERO_HOST, AERO_PORT);
            Producer<String, byte[]> producer = createKafkaProducer();

            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages sent : " + messagesSentThisSecond.get());
                messagesSentThisSecond.set(0); // Reset bộ đếm
            }, 0, 1, TimeUnit.SECONDS);

            while (true) {
                long windowStart = lastPolledTime + 1;
                long windowEnd = System.currentTimeMillis();
                System.out.printf("Scanning window [%d → %d]%n", windowStart, windowEnd);
                try {
                    Statement stmt = new Statement();
                    stmt.setNamespace(NAMESPACE);
                    stmt.setSetName(SET_NAME);
                    // Không dùng filter, quét toàn bộ set
                    // stmt.setFilter(Filter.range(BIN_LAST_UPDATE, windowStart, windowEnd));

                    RecordSet records = client.query(null, stmt);
                    try {
                        while (records.next()) {
                            Key key = records.getKey();
                            Record record = records.getRecord();
                            long updateTime = record.getLong(BIN_LAST_UPDATE);
                            if (updateTime >= windowStart && updateTime <= windowEnd) {
                                byte[] data = record.getBytes(BIN_NAME);
                                String message = (data != null) ?
                                    String.format("{\"personData\": \"%s\", \"lastUpdate\": %d}", Base64.getEncoder().encodeToString(data), updateTime) :
                                    String.format("{\"personData\": null, \"lastUpdate\": %d}", updateTime);
                                executor.submit(() -> sendWithRetry(producer, new ProducerRecord<>(KAFKA_TOPIC, key.userKey.toString(), message.getBytes(StandardCharsets.UTF_8)), 0));
                                messagesSentThisSecond.incrementAndGet();
                            }
                        }
                    } finally {
                        records.close();
                    }
                    // Nếu query thành công, cập nhật lastPolledTime
                    lastPolledTime = windowEnd;
                    System.out.printf("Window done. Next start = %d\n", lastPolledTime);
                } catch (Exception e) {
                    // Nếu lỗi, không cập nhật lastPolledTime, sẽ retry lại window này
                    System.err.println("Error during scan, retrying same window:");
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
            executor.shutdown();
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

