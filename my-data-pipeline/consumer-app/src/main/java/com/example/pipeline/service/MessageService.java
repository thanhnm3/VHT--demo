package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Key;
import com.aerospike.client.Bin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import com.example.pipeline.proto.ProtoSubscriberInfo;
import com.example.pipeline.proto.ProtoSubscriber;
import com.example.pipeline.proto.ProtoBalance;
import com.example.pipeline.proto.ProtoAcmBalance;
import com.example.pipeline.proto.ProtoProduct;
import com.example.pipeline.proto.ProtoCharacteristic;
import com.example.pipeline.proto.ProtoHistory;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

public class MessageService {
    private static final Logger logger = LoggerFactory.getLogger(MessageService.class);
    private final AerospikeClient destinationClient;
    private final WritePolicy writePolicy;
    private final ExecutorService workerPool;
    private final String namespace;
    private final String setName;
    private final String region;
    private volatile boolean isRunning = true;
    private final AtomicLong lastProcessedOffset = new AtomicLong(0);
    private final AtomicLong currentOffset = new AtomicLong(0);
    private final AtomicLong processedRecords = new AtomicLong(0);

    public MessageService(AerospikeClient destinationClient, WritePolicy writePolicy, 
                         String namespace, String setName, String region,
                         int workerPoolSize) {
        this.destinationClient = destinationClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.region = region;
        
        this.workerPool = new ThreadPoolExecutor(
            workerPoolSize,
            workerPoolSize,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            new ThreadFactory() {
                private final AtomicInteger threadCount = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName(region + "-worker-" + threadCount.getAndIncrement());
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void processRecords(ConsumerRecords<byte[], byte[]> records) {
        if (!isRunning || records.isEmpty()) return;

        List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            recordList.add(record);
            currentOffset.set(record.offset());
        }

        processBatch(recordList);
    }

    private void processBatch(List<ConsumerRecord<byte[], byte[]>> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (!isRunning) break;

            workerPool.submit(() -> {
                try {
                    processRecord(record);
                    lastProcessedOffset.set(record.offset());
                    processedRecords.incrementAndGet();
                } catch (Exception e) {
                    logger.error("[{}] Error processing record: {}", region, e.getMessage());
                }
            });
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            if (record.key() == null || record.value() == null) {
                logger.error("[{}] Error: Record key or value is null", region);
                return;
            }

            // Parse ProtoSubscriberInfo from the record value
            ProtoSubscriberInfo subscriberInfo;
            try {
                subscriberInfo = ProtoSubscriberInfo.parseFrom(record.value());
            } catch (InvalidProtocolBufferException e) {
                logger.error("[{}] Error parsing protobuf message: {}", region, e.getMessage());
                return;
            }
            
            // Create Aerospike key using record key
            Key key = new Key(namespace, setName, record.key());
            
            List<Bin> bins = new ArrayList<>();

            // Process subscriber data
            ProtoSubscriber subscriber = subscriberInfo.getSubscriber();
            Map<String, Object> subscriberData = new HashMap<>();
            subscriberData.put("m", subscriber.getMsisdn());
            subscriberData.put("si", subscriber.getSubId());
            subscriberData.put("ci", subscriber.getCustId());
            subscriberData.put("df", subscriber.getIsDefault());
            subscriberData.put("st", subscriber.getSubType());
            subscriberData.put("ss", subscriber.getStateSet());
            subscriberData.put("pc", subscriber.getPrecharge());
            subscriberData.put("bl", subscriber.getBalanceIdListList());
            subscriberData.put("al", subscriber.getAcmBalanceIdListList());
            subscriberData.put("pl", subscriber.getProductIdListList());
            subscriberData.put("gl", subscriber.getGroupIdListList());
            subscriberData.put("ml", subscriber.getMembershipIdListList());
            subscriberData.put("vl", subscriber.getVpnGroupNameListList());
            subscriberData.put("ms", subscriber.getMapSessionTypeLastestMap());
            subscriberData.put("mp", subscriber.getMainProductId());
            subscriberData.put("cl", subscriber.getCellListList());
            subscriberData.put("li", subscriber.getLangId());
            subscriberData.put("r", subscriber.getRegion());
            subscriberData.put("im", subscriber.getImsi());
            subscriberData.put("ic", subscriber.getIccid());
            subscriberData.put("pw", subscriber.getPassword());
            subscriberData.put("bs", subscriber.getBccsSubId());
            subscriberData.put("bc", subscriber.getBccsCustId());
            subscriberData.put("rt", subscriber.getRegType());
            subscriberData.put("sc", subscriber.getSubcategory());
            subscriberData.put("cid", subscriber.getContractId());
            subscriberData.put("ct", subscriber.getCustType());
            subscriberData.put("cv", subscriber.getCustVip());
            subscriberData.put("zl", subscriber.getZoneListList());
            subscriberData.put("p", subscriber.getProvince());
            bins.add(new Bin("sub", subscriberData));

            // Tách trường lastUpdate ra thành bin riêng
            long lastUpdate = subscriber.getLastUpdate();
            bins.add(new Bin("lu", lastUpdate));

            // Tạo bin "ctrl" từ dữ liệu trong Proto message
            if (!subscriberInfo.getCtrlData().isEmpty()) {
                byte[] ctrlData = subscriberInfo.getCtrlData().toByteArray();
                bins.add(new Bin("ctrl", ctrlData));
                if (logger.isDebugEnabled()) {
                    logger.debug("[MessageService] Created ctrl bin with size: {} bytes from Proto message for key: {}", ctrlData.length, key.userKey);
                }
            }

            // Process balance data
            Map<String, Map<String, Object>> balanceData = new HashMap<>();
            for (Map.Entry<Long, ProtoBalance> entry : subscriberInfo.getBalanceMap().entrySet()) {
                ProtoBalance bal = entry.getValue();
                Map<String, Object> balData = new HashMap<>();
                balData.put("i", bal.getId());
                balData.put("g", bal.getGross());
                balData.put("c", bal.getConsume());
                balData.put("r", bal.getReserve());
                balData.put("t", bal.getBalType());
                balData.put("q", bal.getQuotaMax());
                balData.put("d", bal.getRecurringDay());
                balData.put("o", bal.getOwnerValue());
                balData.put("e", bal.getEffDate());
                balData.put("x", bal.getExpDate());
                balData.put("u", bal.getUpdateDate());
                balData.put("s", bal.getState());
                balData.put("l", bal.getCharIdListList());
                balData.put("v", bal.getLevel());
                balData.put("f", bal.getOf());
                balanceData.put("b" + entry.getKey(), balData);
            }
            bins.add(new Bin("bal", balanceData));

            // Process ACM balance data
            Map<String, Map<String, Object>> acmBalanceData = new HashMap<>();
            for (Map.Entry<Long, ProtoAcmBalance> entry : subscriberInfo.getAcmBalanceMap().entrySet()) {
                ProtoAcmBalance acmBal = entry.getValue();
                Map<String, Object> acmBalData = new HashMap<>();
                acmBalData.put("i", acmBal.getId());
                acmBalData.put("v", acmBal.getValue());
                acmBalData.put("r", acmBal.getReserve());
                acmBalData.put("t", acmBal.getBalType());
                acmBalData.put("b", acmBal.getBillingCycleId());
                acmBalData.put("l", acmBal.getLimit());
                acmBalData.put("e", acmBal.getEffDate());
                acmBalData.put("x", acmBal.getExpDate());
                acmBalData.put("u", acmBal.getUpdateDate());
                acmBalData.put("s", acmBal.getState());
                acmBalData.put("c", acmBal.getCharIdListList());
                acmBalData.put("l", acmBal.getLevel());
                acmBalData.put("f", acmBal.getOf());
                acmBalanceData.put("a" + entry.getKey(), acmBalData);
            }
            bins.add(new Bin("acm", acmBalanceData));

            // Process product data
            Map<String, Map<String, Object>> productData = new HashMap<>();
            for (Map.Entry<Long, ProtoProduct> entry : subscriberInfo.getProductMap().entrySet()) {
                ProtoProduct prod = entry.getValue();
                Map<String, Object> prodData = new HashMap<>();
                prodData.put("i", prod.getId());
                prodData.put("o", prod.getProductOfferingId());
                prodData.put("m", prod.getMemberListList());
                prodData.put("d", prod.getRecurringDay());
                prodData.put("e", prod.getEffDate());
                prodData.put("x", prod.getExpDate());
                prodData.put("u", prod.getUpdateDate());
                prodData.put("s", prod.getState());
                prodData.put("c", prod.getCharIdListList());
                prodData.put("l", prod.getLevel());
                prodData.put("f", prod.getOf());
                productData.put("p" + entry.getKey(), prodData);
            }
            bins.add(new Bin("prd", productData));

            // Process characteristic data
            Map<String, Map<String, Object>> charData = new HashMap<>();
            for (Map.Entry<Long, ProtoCharacteristic> entry : subscriberInfo.getCharacteristicMap().entrySet()) {
                ProtoCharacteristic charac = entry.getValue();
                Map<String, Object> characData = new HashMap<>();
                characData.put("i", charac.getId());
                characData.put("s", charac.getCharSpecId());
                characData.put("b", charac.getBillingCycleId());
                characData.put("v", charac.getValue());
                characData.put("l", charac.getLongValue());
                characData.put("e", charac.getEffDate());
                characData.put("x", charac.getExpDate());
                characData.put("u", charac.getUpdateDate());
                characData.put("s", charac.getState());
                characData.put("c", charac.getCharIdListList());
                characData.put("l", charac.getLevel());
                characData.put("f", charac.getOf());
                charData.put("c" + entry.getKey(), characData);
            }
            bins.add(new Bin("chr", charData));

            // Process history data
            Map<String, Map<String, Object>> historyData = new HashMap<>();
            for (Map.Entry<Long, ProtoHistory> entry : subscriberInfo.getHistoryMap().entrySet()) {
                ProtoHistory hist = entry.getValue();
                Map<String, Object> histData = new HashMap<>();
                histData.put("i", hist.getId());
                histData.put("t", hist.getType());
                histData.put("e", hist.getEffDate());
                histData.put("x", hist.getExpDate());
                histData.put("u", hist.getUpdateDate());
                histData.put("s", hist.getState());
                histData.put("c", hist.getCharIdListList());
                histData.put("l", hist.getLevel());
                histData.put("f", hist.getOf());
                histData.put("n", hist.getContent());
                historyData.put("h" + entry.getKey(), histData);
            }
            bins.add(new Bin("his", historyData));

            // Write all bins to Aerospike without generation checking
            writePolicy.generationPolicy = com.aerospike.client.policy.GenerationPolicy.NONE;
            
            try {
                destinationClient.put(writePolicy, key, bins.toArray(new Bin[0]));
            } catch (com.aerospike.client.AerospikeException e) {
                // Xử lý lỗi memory error
                if (e.getMessage().contains("Server memory error")) {
                    // Thử lại với dữ liệu nhỏ hơn
                    try {
                        // Loại bỏ bin ctrl để giảm kích cỡ
                        bins.removeIf(bin -> "ctrl".equals(bin.name));
                        destinationClient.put(writePolicy, key, bins.toArray(new Bin[0]));
                        logger.info("[{}] Successfully wrote key {} without ctrl bin", region, new String((byte[])key.userKey.getObject()));
                    } catch (com.aerospike.client.AerospikeException retryException) {
                        logger.error("[{}] Failed to write key {} even without ctrl bin: {}", region, new String((byte[])key.userKey.getObject()), retryException.getMessage());
                        throw retryException;
                    }
                } else {
                    logger.error("[{}] Error writing to Aerospike for key: {}, error: {}", region, new String((byte[])key.userKey.getObject()), e.getMessage());
                    throw e;
                }
            }
            
        } catch (Exception e) {
            logger.error("[{}] Error processing record: {}", region, e.getMessage(), e);
            throw e;
        }
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public long getLastProcessedOffset() {
        return lastProcessedOffset.get();
    }

    public void shutdown() {
        isRunning = false;
        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
        }
    }

    public void processRecordsAndWait(ConsumerRecords<byte[], byte[]> records) {
        if (!isRunning || records.isEmpty()) return;
        List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            recordList.add(record);
            currentOffset.set(record.offset());
        }
        CountDownLatch latch = new CountDownLatch(recordList.size());
        for (ConsumerRecord<byte[], byte[]> record : recordList) {
            if (!isRunning) {
                latch.countDown();
                continue;
            }
            workerPool.submit(() -> {
                try {
                    processRecord(record);
                    lastProcessedOffset.set(record.offset());
                    processedRecords.incrementAndGet();
                } catch (Exception e) {
                    logger.error("[{}] Error processing record: {}", region, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
} 
