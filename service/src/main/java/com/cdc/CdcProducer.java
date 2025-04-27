package com.cdc;

import com.aerospike.client.*;
import com.aerospike.client.query.*;
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

    private static long lastPolledTime = System.currentTimeMillis() - 10_000; // Bắt đầu từ 10s trước

    public static void start(String aeroHost, int aeroPort, String namespace, String setName, String binName,
            String binLastUpdate,
            String kafkaBroker, String kafkaTopic, int maxRetries, int threadPoolSize) {
        AerospikeClient client = null;
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger messagesSentThisSecond = new AtomicInteger(0);

        try {
            client = new AerospikeClient(aeroHost, aeroPort);
            Producer<byte[], byte[]> producer = createKafkaProducer(kafkaBroker);

            scheduler.scheduleAtFixedRate(() -> {
                System.out.println("Messages sent : " + messagesSentThisSecond.get());
                messagesSentThisSecond.set(0);
            }, 0, 1, TimeUnit.SECONDS);

            while (true) {
                long windowStart = lastPolledTime + 1;
                long windowEnd = System.currentTimeMillis();
                System.out.printf("Scanning window [%d → %d]%n", windowStart, windowEnd);
                try {
                    Statement stmt = new Statement();
                    stmt.setNamespace(namespace);
                    stmt.setSetName(setName);

                    RecordSet records = client.query(null, stmt);
                    try {
                        while (records.next()) {
                            Key key = records.getKey();
                            Record record = records.getRecord();
                            long updateTime = record.getLong(binLastUpdate);
                            if (updateTime >= windowStart && updateTime <= windowEnd) {
                                byte[] data = record.getBytes(binName);
                                String message = (data != null)
                                        ? String.format("{\"personData\": \"%s\", \"lastUpdate\": %d}",
                                                Base64.getEncoder().encodeToString(data), updateTime)
                                        : String.format("{\"personData\": null, \"lastUpdate\": %d}", updateTime);
                                executor.submit(() -> sendWithRetry(producer, new ProducerRecord<>(kafkaTopic,
                                        (byte[]) key.userKey.getObject(), message.getBytes(StandardCharsets.UTF_8)), maxRetries));
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
                client.close();
                System.out.println("Aerospike client closed.");
            }
            executor.shutdown();
            scheduler.shutdown();
        }
    }

    private static Producer<byte[], byte[]> createKafkaProducer(String kafkaBroker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("linger.ms", "5");
        props.put("batch.size", "32768");
        props.put("acks", "all");
        return new KafkaProducer<>(props);
    }

    private static void sendWithRetry(Producer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record,
            int maxRetries) {
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send message: " + exception.getMessage());
            }
        });
    }
}
