package com.example.pipeline.test;

import com.example.pipeline.proto.ProtoSubscriberInfo;
import com.example.pipeline.proto.ProtoSubscriber;
import com.example.pipeline.proto.ProtoBalance;
import com.example.pipeline.proto.ProtoAcmBalance;
import com.example.pipeline.proto.ProtoProduct;
import com.example.pipeline.proto.ProtoCharacteristic;
import com.example.pipeline.proto.ProtoHistory;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducerServiceTest {
    private static final String TOPIC = "performance-test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    private static final int NUM_MESSAGES = 1000000; // 1 million messages
    private static final int NUM_CONSUMERS = 3;
    private static final int NUM_PRODUCERS = 3;
    private static final int NUM_PARTITIONS = 2;
    private static final short REPLICATION_FACTOR = 2;
    private static final Random random = new Random();
    private static final String[] REGIONS = {"NORTH", "SOUTH", "CENTRAL", "EAST", "WEST"};
    private static final String[] PROVINCES = {"HANOI", "HCM", "DANANG", "HAIPHONG", "CANTHO"};
    private static final String[] SERVICE_TYPES = {"SMS", "VOICE", "DATA", "VAS", "ROAMING"};

    public static void main(String[] args) throws InterruptedException {
        // Setup topic
        setupTopic();

        // Run producers
        System.out.println("Starting producers...");
        CountDownLatch producerLatch = new CountDownLatch(NUM_PRODUCERS);
        ExecutorService producerExecutor = Executors.newFixedThreadPool(NUM_PRODUCERS);
        AtomicLong totalProduced = new AtomicLong(0);
        AtomicLong totalBytesProduced = new AtomicLong(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_PRODUCERS; i++) {
            producerExecutor.submit(() -> {
                try {
                    Producer<byte[], byte[]> producer = createProducer();
                    long produced = 0;
                    long bytesProduced = 0;
                    for (int j = 0; j < NUM_MESSAGES / NUM_PRODUCERS; j++) {
                        ProducerRecord<byte[], byte[]> record = createKafkaRecord();
                        if (record != null) {
                            producer.send(record);
                            produced++;
                            bytesProduced += record.value().length;
                        }
                    }
                    producer.flush();
                    producer.close();
                    totalProduced.addAndGet(produced);
                    totalBytesProduced.addAndGet(bytesProduced);
                } finally {
                    producerLatch.countDown();
                }
            });
        }

        producerLatch.await();
        long producerEndTime = System.currentTimeMillis();
        long producerDuration = producerEndTime - startTime;
        System.out.printf("Produced %d messages (%.2f MB) in %d ms%n", 
            totalProduced.get(), 
            totalBytesProduced.get() / (1024.0 * 1024.0),
            producerDuration);
        System.out.printf("Throughput: %.2f msgs/sec, %.2f MB/sec%n", 
            (totalProduced.get() * 1000.0) / producerDuration,
            (totalBytesProduced.get() * 1000.0) / (1024.0 * 1024.0 * producerDuration));

        // Run consumers
        System.out.println("\nStarting consumers...");
        CountDownLatch consumerLatch = new CountDownLatch(NUM_CONSUMERS);
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMERS);
        AtomicLong totalConsumed = new AtomicLong(0);
        AtomicLong totalBytesConsumed = new AtomicLong(0);
        startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_CONSUMERS; i++) {
            final int consumerId = i;
            consumerExecutor.submit(() -> {
                try {
                    System.out.println("Consumer " + consumerId + " starting...");
                    Consumer<byte[], byte[]> consumer = createConsumer();
                    consumer.subscribe(Collections.singletonList(TOPIC));
                    long consumed = 0;
                    long bytesConsumed = 0;
                    long lastLogTime = System.currentTimeMillis();
                    long noNewMessagesCount = 0;
                    long lastConsumedCount = 0;
                    
                    while (consumed < NUM_MESSAGES / NUM_CONSUMERS) {
                        try {
                            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
                            
                            if (records.isEmpty()) {
                                noNewMessagesCount++;
                                if (noNewMessagesCount >= 5) { // 5 seconds without new messages
                                    System.out.println("Consumer " + consumerId + " no new messages for 5 seconds, checking if we're done...");
                                    if (consumed >= (NUM_MESSAGES / NUM_CONSUMERS) - 1000) { // Allow small difference
                                        System.out.println("Consumer " + consumerId + " close to target, considering complete");
                                        break;
                                    }
                                }
                            } else {
                                noNewMessagesCount = 0;
                            }
                            
                            for (ConsumerRecord<byte[], byte[]> record : records) {
                                consumed++;
                                bytesConsumed += record.value().length;
                            }
                            
                            // Log progress every 5 seconds or when significant change
                            long currentTime = System.currentTimeMillis();
                            if (currentTime - lastLogTime > 5000 || (consumed - lastConsumedCount) > 10000) {
                                System.out.printf("Consumer %d: Consumed %d/%d messages (%.2f MB)%n", 
                                    consumerId,
                                    consumed,
                                    NUM_MESSAGES / NUM_CONSUMERS,
                                    bytesConsumed / (1024.0 * 1024.0));
                                lastLogTime = currentTime;
                                lastConsumedCount = consumed;
                            }
                        } catch (Exception e) {
                            System.err.println("Consumer " + consumerId + " error: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                    
                    System.out.println("Consumer " + consumerId + " completed. Total consumed: " + consumed);
                    consumer.close();
                    totalConsumed.addAndGet(consumed);
                    totalBytesConsumed.addAndGet(bytesConsumed);
                } catch (Exception e) {
                    System.err.println("Consumer " + consumerId + " failed: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    consumerLatch.countDown();
                }
            });
        }

        consumerLatch.await();
        long consumerEndTime = System.currentTimeMillis();
        long consumerDuration = consumerEndTime - startTime;
        System.out.printf("Consumed %d messages (%.2f MB) in %d ms%n", 
            totalConsumed.get(), 
            totalBytesConsumed.get() / (1024.0 * 1024.0),
            consumerDuration);
        System.out.printf("Throughput: %.2f msgs/sec, %.2f MB/sec%n", 
            (totalConsumed.get() * 1000.0) / consumerDuration,
            (totalBytesConsumed.get() * 1000.0) / (1024.0 * 1024.0 * consumerDuration));

        producerExecutor.shutdown();
        consumerExecutor.shutdown();
    }

    private static ProducerRecord<byte[], byte[]> createKafkaRecord() {
        try {
            // Create ProtoSubscriber
            ProtoSubscriber subscriber = ProtoSubscriber.newBuilder()
                .setMsisdn("09" + String.format("%08d", random.nextInt(100000000)))
                .setSubId(random.nextLong())
                .setCustId(random.nextLong())
                .setIsDefault(random.nextBoolean())
                .setSubType(random.nextInt(5))
                .setStateSet("ACTIVE")
                .setPrecharge(random.nextLong())
                .addAllBalanceIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .addAllAcmBalanceIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .addAllProductIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .addAllGroupIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .addAllMembershipIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .addAllVpnGroupNameList(Arrays.asList("VPN1", "VPN2"))
                .putMapSessionTypeLastest(1, "LAST_SESSION")
                .setMainProductId(random.nextLong())
                .addAllCellList(Arrays.asList("CELL1", "CELL2"))
                .setLangId(random.nextInt(3))
                .setRegion(REGIONS[random.nextInt(REGIONS.length)])
                .setLastUpdate(System.currentTimeMillis())
                .setImsi("IMSI" + random.nextInt(1000000))
                .setIccid("ICCID" + random.nextInt(1000000))
                .setPassword("PASS" + random.nextInt(1000000))
                .setBccsSubId("BCCS" + random.nextInt(1000000))
                .setBccsCustId("CUST" + random.nextInt(1000000))
                .setBccsAcctId("ACCT" + random.nextInt(1000000))
                .setRegType("PREPAID")
                .setSubcategory("INDIVIDUAL")
                .setContractId("CONT" + random.nextInt(1000000))
                .setCustType("RESIDENTIAL")
                .setCustVip("NORMAL")
                .addAllZoneList(Arrays.asList("ZONE1", "ZONE2"))
                .setProvince(PROVINCES[random.nextInt(PROVINCES.length)])
                .setEmail("user" + random.nextInt(1000000) + "@example.com")
                .setAddress("Address " + random.nextInt(1000))
                .setFirstName("First" + random.nextInt(1000))
                .setLastName("Last" + random.nextInt(1000))
                .setCompleteDate(System.currentTimeMillis())
                .setBirthDay(System.currentTimeMillis() - (random.nextInt(365) * 24 * 60 * 60 * 1000L))
                .setStartNotifyTime(System.currentTimeMillis())
                .setEndNotifyTime(System.currentTimeMillis() + (random.nextInt(365) * 24 * 60 * 60 * 1000L))
                .setCountTopupTotal(random.nextLong())
                .setCountTopupFailure(random.nextLong())
                .setCountTopupSuccess(random.nextLong())
                .setSex(random.nextInt(2))
                .setEffDate(System.currentTimeMillis())
                .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                .setUpdateDate(System.currentTimeMillis())
                .setState(1)
                .addAllCharIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .setLevel(random.nextInt(5))
                .setOf(random.nextLong())
                .putMapExtProp("key1", "value1")
                .putMapExtProp("key2", "value2")
                .build();

            // Create ProtoBalance
            ProtoBalance balance = ProtoBalance.newBuilder()
                .setId(random.nextLong())
                .setGross(random.nextLong())
                .setConsume(random.nextLong())
                .setReserve(random.nextLong())
                .setBalType(random.nextLong())
                .setQuotaMax(random.nextLong())
                .setRecurringDay(random.nextLong())
                .setOwnerValue("OWNER" + random.nextInt(1000000))
                .setEffDate(System.currentTimeMillis())
                .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                .setUpdateDate(System.currentTimeMillis())
                .setState(1)
                .addAllCharIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .setLevel(random.nextInt(5))
                .setOf(random.nextLong())
                .putMapExtProp("key1", "value1")
                .build();

            // Create ProtoAcmBalance
            ProtoAcmBalance acmBalance = ProtoAcmBalance.newBuilder()
                .setId(random.nextLong())
                .setValue(random.nextLong())
                .setReserve(random.nextLong())
                .setBalType(random.nextLong())
                .setBillingCycleId(random.nextLong())
                .setLimit(random.nextLong())
                .setEffDate(System.currentTimeMillis())
                .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                .setUpdateDate(System.currentTimeMillis())
                .setState(1)
                .addAllCharIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .setLevel(random.nextInt(5))
                .setOf(random.nextLong())
                .putMapExtProp("key1", "value1")
                .build();

            // Create ProtoProduct
            ProtoProduct product = ProtoProduct.newBuilder()
                .setId(random.nextLong())
                .setProductOfferingId(random.nextLong())
                .addAllMemberList(Arrays.asList("MEMBER1", "MEMBER2"))
                .setRecurringDay(random.nextLong())
                .setEffDate(System.currentTimeMillis())
                .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                .setUpdateDate(System.currentTimeMillis())
                .setState(1)
                .addAllCharIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .setLevel(random.nextInt(5))
                .setOf(random.nextLong())
                .putMapExtProp("key1", "value1")
                .build();

            // Create ProtoCharacteristic
            ProtoCharacteristic characteristic = ProtoCharacteristic.newBuilder()
                .setId(random.nextLong())
                .setCharSpecId(random.nextLong())
                .setBillingCycleId(random.nextLong())
                .setValue("VALUE" + random.nextInt(1000000))
                .setLongValue(random.nextLong())
                .setEffDate(System.currentTimeMillis())
                .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                .setUpdateDate(System.currentTimeMillis())
                .setState(1)
                .addAllCharIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .setLevel(random.nextInt(5))
                .setOf(random.nextLong())
                .putMapExtProp("key1", "value1")
                .build();

            // Create ProtoHistory
            ProtoHistory history = ProtoHistory.newBuilder()
                .setId(random.nextLong())
                .setType(random.nextInt(5))
                .setEffDate(System.currentTimeMillis())
                .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                .setUpdateDate(System.currentTimeMillis())
                .setState(1)
                .addAllCharIdList(Arrays.asList(random.nextLong(), random.nextLong()))
                .setLevel(random.nextInt(5))
                .setOf(random.nextLong())
                .putMapExtProp("key1", "value1")
                .setContent("History content " + random.nextInt(1000000))
                .build();

            // Create ProtoSubscriberInfo
            ProtoSubscriberInfo subscriberInfo = ProtoSubscriberInfo.newBuilder()
                .setSubscriber(subscriber)
                .putBalance(random.nextLong(), balance)
                .putAcmBalance(random.nextLong(), acmBalance)
                .putProduct(random.nextLong(), product)
                .putCharacteristic(random.nextLong(), characteristic)
                .putHistory(random.nextLong(), history)
                .setGeneration(random.nextInt(10))
                .build();

            // Serialize to bytes
            byte[] keyBytes = subscriber.getMsisdn().getBytes();
            byte[] valueBytes = subscriberInfo.toByteArray();

            return new ProducerRecord<>(TOPIC, keyBytes, valueBytes);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static Producer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return new KafkaProducer<>(props);
    }

    private static Consumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "performance-test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50000);
        return new KafkaConsumer<>(props);
    }

    private static void setupTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        try (AdminClient admin = AdminClient.create(props)) {
            // Delete topic if exists
            try {
                System.out.println("Deleting existing topic: " + TOPIC);
                admin.deleteTopics(Collections.singletonList(TOPIC)).all().get();
                System.out.println("Topic deleted successfully");
            } catch (Exception e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    System.out.println("Topic does not exist, proceeding with creation");
                } else {
                    System.err.println("Error deleting topic: " + e.getMessage());
                    throw e;
                }
            }

            // Create new topic
            System.out.println("Creating new topic: " + TOPIC);
            NewTopic newTopic = new NewTopic(TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
            admin.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Topic created successfully with " + NUM_PARTITIONS + " partitions and " + REPLICATION_FACTOR + " replicas");

            // Wait for topic to be ready
            System.out.println("Waiting for topic to be ready...");
            Thread.sleep(5000); // Wait for 5 seconds to ensure topic is ready
        } catch (Exception e) {
            System.err.println("Error setting up topic: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
} 