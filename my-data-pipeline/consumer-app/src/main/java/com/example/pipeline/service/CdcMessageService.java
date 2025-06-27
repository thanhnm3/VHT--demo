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
import java.util.concurrent.ConcurrentLinkedQueue;
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

/**
 * CDC Message Service với tính năng monitoring độ trễ CDC
 * Tính toán độ trễ giữa thời điểm last update (lu) và thời điểm nhận được message
 * Sử dụng sliding window với 20,000 bản ghi để tính trung bình
 * Chỉ log một lần khi đạt đủ 20,000 bản ghi
 */
public class CdcMessageService implements MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CdcMessageService.class);
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
    
    // CDC Latency Monitor - tích hợp trực tiếp
    private final ConcurrentLinkedQueue<Long> latencyQueue;
    private final AtomicLong totalLatency;
    private final AtomicLong recordCount;
    private final int windowSize;
    private volatile boolean isLatencyMonitoringRunning = true;
    private volatile boolean hasLoggedResult = false; // Để đảm bảo chỉ log một lần

    public CdcMessageService(AerospikeClient destinationClient, WritePolicy writePolicy, 
                         String namespace, String setName, String region,
                         int workerPoolSize) {
        this.destinationClient = destinationClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.region = region;
        
        // Khởi tạo CDC Latency Monitor với window size 20,000
        this.windowSize = 10000;
        this.latencyQueue = new ConcurrentLinkedQueue<>();
        this.totalLatency = new AtomicLong(0);
        this.recordCount = new AtomicLong(0);
        
        // Bắt đầu monitoring
        startLatencyMonitoring();
        
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
                    thread.setName(region + "-cdc-worker-" + threadCount.getAndIncrement());
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        logger.info("[CDC MessageService] Initialized with latency monitoring (window size: {})", windowSize);
    }

    /**
     * Bắt đầu monitoring độ trễ CDC
     */
    private void startLatencyMonitoring() {
        // Không cần scheduled task nữa, chỉ log khi đạt đủ 20,000 bản ghi
        logger.info("[CDC Latency Monitor] Started monitoring with window size: {}", windowSize);
    }

    /**
     * Ghi lại độ trễ CDC
     */
    private void recordLatency(long lastUpdateTime, long receiveTime) {
        if (!isLatencyMonitoringRunning || hasLoggedResult) return;
        
        long latency = receiveTime - lastUpdateTime;
        if (latency >= 0) { // Chỉ tính latency hợp lệ
            latencyQueue.offer(latency);
            totalLatency.addAndGet(latency);
            long currentCount = recordCount.incrementAndGet();
            
            // Log tiến độ mỗi 1000 records
            if (currentCount % 1000 == 0) {
                logger.info("[CDC Latency Monitor] Progress: {}/{} records collected", currentCount, windowSize);
            }
            
            // Nếu queue đã đầy, loại bỏ phần tử cũ nhất
            if (latencyQueue.size() > windowSize) {
                Long removedLatency = latencyQueue.poll();
                if (removedLatency != null) {
                    totalLatency.addAndGet(-removedLatency);
                }
            }
            
            // Chỉ log một lần khi đạt đủ 20,000 bản ghi
            if (currentCount == windowSize && !hasLoggedResult) {
                hasLoggedResult = true; // Đánh dấu đã log
                logAverageLatency();
                logger.info("[CDC Latency Monitor] Monitoring completed. Stopping further latency calculations.");
            }
        }
    }

    /**
     * Log trung bình độ trễ
     */
    private void logAverageLatency() {
        int currentSize = latencyQueue.size();
        if (currentSize == 0) return;
        
        long currentTotal = totalLatency.get();
        double averageLatency = (double) currentTotal / currentSize;
        
        logger.info("[CDC Latency Monitor] =========================================");
        logger.info("[CDC Latency Monitor] FINAL RESULT - CDC Latency Analysis");
        logger.info("[CDC Latency Monitor] =========================================");
        logger.info("[CDC Latency Monitor] Total records processed: {}", recordCount.get());
        logger.info("[CDC Latency Monitor] Window size: {}", windowSize);
        logger.info("[CDC Latency Monitor] Average latency: {}ms", String.format("%.2f", averageLatency));
        logger.info("[CDC Latency Monitor] =========================================");
    }

    @Override
    public void processRecords(ConsumerRecords<byte[], byte[]> records) {
        if (!isRunning || records.isEmpty()) return;

        // Log để kiểm tra xem có records được nhận không - đổi thành DEBUG
        logger.debug("[{}] Received {} records from Kafka", region, records.count());

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

            // Lấy thời gian nhận được message
            long receiveTime = System.currentTimeMillis();

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
            
            // Tính toán và ghi lại latency CDC
            recordLatency(lastUpdate, receiveTime);

            // Tạo bin "ctrl" từ dữ liệu trong Proto message
            if (!subscriberInfo.getCtrlData().isEmpty()) {
                byte[] ctrlData = subscriberInfo.getCtrlData().toByteArray();
                bins.add(new Bin("ctrl", ctrlData));
                if (logger.isDebugEnabled()) {
                    logger.debug("[CdcMessageService] Created ctrl bin with size: {} bytes from Proto message for key: {}", ctrlData.length, key.userKey);
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
                if (e.getResultCode() == com.aerospike.client.ResultCode.DEVICE_OVERLOAD) {
                    logger.warn("[{}] Memory overload detected, retrying after delay: {}", region, e.getMessage());
                    Thread.sleep(100); // Đợi 100ms trước khi thử lại
                    destinationClient.put(writePolicy, key, bins.toArray(new Bin[0]));
                } else {
                    throw e;
                }
            }
            
        } catch (Exception e) {
            logger.error("[{}] Error processing record: {}", region, e.getMessage(), e);
        }
    }

    @Override
    public void processRecordsAndWait(ConsumerRecords<byte[], byte[]> records) {
        if (!isRunning || records.isEmpty()) return;

        List<ConsumerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            recordList.add(record);
            currentOffset.set(record.offset());
        }

        CountDownLatch latch = new CountDownLatch(recordList.size());
        
        for (ConsumerRecord<byte[], byte[]> record : recordList) {
            if (!isRunning) break;

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
            logger.error("[{}] Interrupted while waiting for batch processing", region);
        }
    }

    @Override
    public long getCurrentOffset() {
        return currentOffset.get();
    }

    @Override
    public long getLastProcessedOffset() {
        return lastProcessedOffset.get();
    }

    /**
     * Lấy trung bình độ trễ hiện tại
     */
    public double getCurrentAverageLatency() {
        int currentSize = latencyQueue.size();
        if (currentSize == 0) return 0.0;
        
        long currentTotal = totalLatency.get();
        return (double) currentTotal / currentSize;
    }

    /**
     * Lấy số lượng bản ghi hiện tại trong window
     */
    public int getCurrentRecordCount() {
        return latencyQueue.size();
    }

    @Override
    public void shutdown() {
        isRunning = false;
        isLatencyMonitoringRunning = false;
        
        // Log final latency statistics nếu có dữ liệu
        int finalSize = latencyQueue.size();
        if (finalSize > 0) {
            long finalTotal = totalLatency.get();
            double finalAverage = (double) finalTotal / finalSize;
            logger.info("[CDC Latency Monitor] Final statistics - Average latency: {:.2f}ms over {} records", 
                       finalAverage, finalSize);
        }
        
        // Shutdown worker pool
        if (workerPool != null) {
            workerPool.shutdown();
            try {
                if (!workerPool.awaitTermination(60, TimeUnit.SECONDS)) {
                    workerPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                workerPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        logger.info("[{}] CDC MessageService shutdown completed", region);
    }
} 