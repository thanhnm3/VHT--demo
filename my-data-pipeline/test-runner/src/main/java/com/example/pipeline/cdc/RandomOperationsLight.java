package com.example.pipeline.cdc;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.google.common.util.concurrent.RateLimiter;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ScheduledExecutorService;

public class RandomOperationsLight {

    private static final String[] REGIONS = {
        "north", "central", "south"
    };

    private static final int KEY_LIMIT = 50_000;
    private static final int OPERATIONS_PER_SECOND_PER_REGION = 2000;
    private static final Map<String, Integer> REGION_LIMITS = Map.of(
        "north", 5_000,
        "central", 5_000,
        "south", 5_000
    );
    private static final Map<String, Map<String, AtomicInteger>> OPERATION_COUNTERS = new HashMap<>();
    private static final Map<String, RateLimiter> REGION_RATE_LIMITERS = new HashMap<>();
    private static boolean loggedInsert = false;
    private static boolean loggedUpdate = false;
    private static boolean loggedDelete = false;

    public static void main(String aeroHost, int aeroPort, String namespace, String setName, int operationsPerSecond,
            int threadPoolSize) {
        // Load configuration
        Config config = ConfigLoader.getConfig();
        if (config == null) {
            throw new IllegalStateException("Cannot load configuration");
        }

        // Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient(aeroHost, aeroPort);
        WritePolicy writePolicy = new WritePolicy();
        Policy readPolicy = new Policy();
        writePolicy.sendKey = true;

        Random random = new Random();

        // Khởi tạo bộ đếm cho mỗi region và mỗi loại thao tác
        for (String region : REGIONS) {
            Map<String, AtomicInteger> regionCounters = new HashMap<>();
            regionCounters.put("insert", new AtomicInteger(0));
            regionCounters.put("update", new AtomicInteger(0));
            regionCounters.put("delete", new AtomicInteger(0));
            OPERATION_COUNTERS.put(region, regionCounters);
            
            // Khởi tạo RateLimiter cho mỗi region
            REGION_RATE_LIMITERS.put(region, RateLimiter.create(OPERATIONS_PER_SECOND_PER_REGION));
        }

        // Sử dụng RateLimiter để kiểm soát tốc độ tổng thể
        RateLimiter rateLimiter = RateLimiter.create(operationsPerSecond);

        AtomicInteger totalInsertCount = new AtomicInteger(0);
        AtomicInteger totalUpdateCount = new AtomicInteger(0);
        AtomicInteger totalDeleteCount = new AtomicInteger(0);

        // Tính tổng số thao tác tối đa
        int totalMaxOps = REGION_LIMITS.values().stream().mapToInt(Integer::intValue).sum() * 3;

        // Thêm biến đếm thao tác mỗi giây
        AtomicInteger opsThisSecond = new AtomicInteger(0);
        // Thêm biến đếm thao tác mỗi giây cho từng loại thao tác và từng region
        Map<String, AtomicInteger> insertThisSecond = new HashMap<>();
        Map<String, AtomicInteger> updateThisSecond = new HashMap<>();
        Map<String, AtomicInteger> deleteThisSecond = new HashMap<>();
        for (String region : REGIONS) {
            insertThisSecond.put(region, new AtomicInteger(0));
            updateThisSecond.put(region, new AtomicInteger(0));
            deleteThisSecond.put(region, new AtomicInteger(0));
        }
        ScheduledExecutorService statsLogger = Executors.newSingleThreadScheduledExecutor();
        statsLogger.scheduleAtFixedRate(() -> {
            int totalOps = totalInsertCount.get() + totalUpdateCount.get() + totalDeleteCount.get();
            for (String region : REGIONS) {
                int ins = insertThisSecond.get(region).getAndSet(0);
                int upd = updateThisSecond.get(region).getAndSet(0);
                int del = deleteThisSecond.get(region).getAndSet(0);
                System.out.println("[RandomOpsLight] Mien " + region + ": Insert=" + ins + ", Update=" + upd + ", Delete=" + del);
            }
            System.out.println("[RandomOpsLight] Da thuc hien: " + totalOps + " / " + totalMaxOps);
        }, 1, 1, TimeUnit.SECONDS);

        // Lấy danh sách key từ database
        ConcurrentLinkedQueue<Key> randomKeys = getRandomKeysFromDatabase(client, namespace, setName, KEY_LIMIT);
        System.out.println("Retrieved " + randomKeys.size() + " keys from database");

        // Tạo thread pool
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);

        // Kiểm tra xem tất cả region và loại thao tác đã đạt giới hạn chưa
        boolean allOperationsReachedLimit = false;
        while (!allOperationsReachedLimit) {
            rateLimiter.acquire();

            executor.submit(() -> {
                int operationType = random.nextInt(3); // 0: Insert, 1: Update, 2: Delete
                String operationName = operationType == 0 ? "insert" : 
                                     operationType == 1 ? "update" : "delete";
                boolean operationPerformed = false;
                
                // Chọn region ngẫu nhiên
                String region = REGIONS[random.nextInt(REGIONS.length)];
                
                // Kiểm tra giới hạn cho region và loại thao tác
                if (OPERATION_COUNTERS.get(region).get(operationName).get() >= REGION_LIMITS.get(region)) {
                    return;
                }

                // Kiểm tra rate limit cho region
                if (!REGION_RATE_LIMITERS.get(region).tryAcquire()) {
                    return;
                }
                
                switch (operationType) {
                    case 0: // Insert
                        operationPerformed = performInsert(client, writePolicy, namespace, setName, random, region, config);
                        if (operationPerformed) {
                            totalInsertCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("insert").incrementAndGet();
                            opsThisSecond.incrementAndGet();
                            insertThisSecond.get(region).incrementAndGet();
                        }
                        break;
                    case 1: // Update
                        operationPerformed = performUpdate(client, writePolicy, readPolicy, namespace, setName, randomKeys, random, region, config);
                        if (operationPerformed) {
                            totalUpdateCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("update").incrementAndGet();
                            opsThisSecond.incrementAndGet();
                            updateThisSecond.get(region).incrementAndGet();
                        }
                        break;
                    case 2: // Delete
                        operationPerformed = performDelete(client, namespace, setName, randomKeys, region);
                        if (operationPerformed) {
                            totalDeleteCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("delete").incrementAndGet();
                            opsThisSecond.incrementAndGet();
                            deleteThisSecond.get(region).incrementAndGet();
                        }
                        break;
                }
            });

            // Kiểm tra xem tất cả region và loại thao tác đã đạt giới hạn chưa
            allOperationsReachedLimit = true;
            for (Map.Entry<String, Map<String, AtomicInteger>> regionEntry : OPERATION_COUNTERS.entrySet()) {
                String region = regionEntry.getKey();
                Map<String, AtomicInteger> operations = regionEntry.getValue();
                for (AtomicInteger counter : operations.values()) {
                    if (counter.get() < REGION_LIMITS.get(region)) {
                        allOperationsReachedLimit = false;
                        break;
                    }
                }
                if (!allOperationsReachedLimit) break;
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            System.err.println("Thread interrupted: " + e.getMessage());
        }

        // Shutdown statsLogger
        statsLogger.shutdown();

        System.out.println("\n=== Operation Results (Light Version) ===");
        System.out.println("Total operations:");
        System.out.println("Insert: " + totalInsertCount.get());
        System.out.println("Update: " + totalUpdateCount.get());
        System.out.println("Delete: " + totalDeleteCount.get());
        System.out.println("\nChanges by region:");
        for (Map.Entry<String, Map<String, AtomicInteger>> regionEntry : OPERATION_COUNTERS.entrySet()) {
            String region = regionEntry.getKey();
            Map<String, AtomicInteger> operations = regionEntry.getValue();
            System.out.printf("  %s:\n", region);
            System.out.printf("    Insert: %d/%d\n", 
                operations.get("insert").get(), REGION_LIMITS.get(region));
            System.out.printf("    Update: %d/%d\n", 
                operations.get("update").get(), REGION_LIMITS.get(region));
            System.out.printf("    Delete: %d/%d\n", 
                operations.get("delete").get(), REGION_LIMITS.get(region));
        }
        System.out.println("=========================================");

        client.close();
    }

    private static boolean performInsert(AerospikeClient client, WritePolicy writePolicy, String namespace, String setName,
            Random random, String region, Config config) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("insert").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        // Generate UUID for user_id
        String userId = UUID.randomUUID().toString();
        byte[] userIdBytes = userId.getBytes();
        Key key = new Key(namespace, setName, userIdBytes);

        // Kiểm tra xem key đã tồn tại chưa
        try {
            Record existingRecord = client.get(null, key);
            if (existingRecord != null) {
                return false;
            }
        } catch (AerospikeException e) {
            return false;
        }

        // Random phone number
        String phoneNumber = String.format("09%d", ThreadLocalRandom.current().nextInt(10000000, 100000000));
        // Random province from this region
        List<String> provinces = config.getRegion_groups().getProvincesByRegion(region);
        String province = provinces.get(random.nextInt(provinces.size()));

        // === Sinh map cho từng bin giống RandomInsertLight ===
        // sub - chỉ giữ các trường cần thiết
        Map<String, Object> sub = new HashMap<>();
        sub.put("m", phoneNumber);
        sub.put("si", random.nextLong());
        sub.put("ci", random.nextLong());
        sub.put("df", random.nextBoolean());
        sub.put("st", random.nextInt(5));
        sub.put("ss", "ACTIVE");
        sub.put("pc", random.nextLong());
        sub.put("r", region);
        sub.put("im", "IMSI" + random.nextInt(1000000));
        sub.put("rt", "PREPAID");
        sub.put("sc", "INDIVIDUAL");
        sub.put("ct", "RESIDENTIAL");
        sub.put("p", province);
        sub.put("em", "user" + random.nextInt(1000000) + "@example.com");
        sub.put("fn", "First" + random.nextInt(1000));
        sub.put("ln", "Last" + random.nextInt(1000));
        sub.put("sx", random.nextInt(2));
        sub.put("e", System.currentTimeMillis());
        sub.put("x", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
        sub.put("s", 1);
        Bin subBin = new Bin("sub", sub);

        // Tách trường lastUpdate ra thành bin riêng
        long lastUpdate = System.currentTimeMillis();
        Bin luBin = new Bin("lu", lastUpdate);

        // Thêm bin để kiểm soát kích cỡ dữ liệu (nhỏ hơn)
        int ctrlSize = ThreadLocalRandom.current().nextInt(5, 10);
        byte[] randomBytes = new byte[ctrlSize];
        ThreadLocalRandom.current().nextBytes(randomBytes);
        Bin ctrlBin = new Bin("ctrl", randomBytes);

        // bal - chỉ giữ các trường cần thiết
        Map<String, Map<String, Object>> bal = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> balEntry = new HashMap<>();
            balEntry.put("i", random.nextLong());
            balEntry.put("g", random.nextLong());
            balEntry.put("c", random.nextLong());
            balEntry.put("r", random.nextLong());
            balEntry.put("t", random.nextLong());
            balEntry.put("e", System.currentTimeMillis());
            balEntry.put("x", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
            balEntry.put("s", 1);
            bal.put("b" + i, balEntry);
        }
        Bin balBin = new Bin("bal", bal);

        // acm - chỉ giữ các trường cần thiết
        Map<String, Map<String, Object>> acm = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> acmEntry = new HashMap<>();
            acmEntry.put("i", random.nextLong());
            acmEntry.put("v", random.nextLong());
            acmEntry.put("r", random.nextLong());
            acmEntry.put("t", random.nextLong());
            acmEntry.put("e", System.currentTimeMillis());
            acmEntry.put("x", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
            acmEntry.put("s", 1);
            acm.put("a" + i, acmEntry);
        }
        Bin acmBin = new Bin("acm", acm);

        // prd - chỉ giữ các trường cần thiết
        Map<String, Map<String, Object>> prd = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> prdEntry = new HashMap<>();
            prdEntry.put("i", random.nextLong());
            prdEntry.put("o", random.nextLong());
            prdEntry.put("e", System.currentTimeMillis());
            prdEntry.put("x", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
            prdEntry.put("s", 1);
            prd.put("p" + i, prdEntry);
        }
        Bin prdBin = new Bin("prd", prd);

        // chr - chỉ giữ các trường cần thiết
        Map<String, Map<String, Object>> chr = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> chrEntry = new HashMap<>();
            chrEntry.put("i", random.nextLong());
            chrEntry.put("s", random.nextLong());
            chrEntry.put("v", "VALUE" + random.nextInt(1000000));
            chrEntry.put("e", System.currentTimeMillis());
            chrEntry.put("x", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
            chrEntry.put("s", 1);
            chr.put("c" + i, chrEntry);
        }
        Bin chrBin = new Bin("chr", chr);

        // his - chỉ giữ các trường cần thiết
        Map<String, Map<String, Object>> his = new HashMap<>();
        for (int i = 0; i < 2; i++) {
            Map<String, Object> hisEntry = new HashMap<>();
            hisEntry.put("i", random.nextLong());
            hisEntry.put("t", random.nextInt(5));
            hisEntry.put("e", System.currentTimeMillis());
            hisEntry.put("x", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
            hisEntry.put("s", 1);
            hisEntry.put("n", "History content " + random.nextInt(1000000));
            his.put("h" + i, hisEntry);
        }
        Bin hisBin = new Bin("his", his);

        // gen
        Bin genBin = new Bin("gen", random.nextInt(10));

        try {
            client.put(writePolicy, key, subBin, balBin, acmBin, prdBin, chrBin, hisBin, genBin, luBin, ctrlBin);
            if (!loggedInsert) {
                Object keyObj = key.userKey != null ? key.userKey.getObject() : null;
                String keyStr = keyObj instanceof byte[] ? bytesToHex((byte[]) keyObj) : String.valueOf(keyObj);
                System.out.println("[INSERT] Key: " + keyStr + " (Light version)");
                loggedInsert = true;
            }
            return true;
        } catch (AerospikeException e) {
            System.err.println("Error inserting record with key: " + key.userKey + " (error: " + e.getMessage() + ")");
            return false;
        }
    }

    private static boolean performUpdate(AerospikeClient client, WritePolicy writePolicy, Policy readPolicy,
            String namespace, String setName, ConcurrentLinkedQueue<Key> randomKeys, Random random, String region, Config config) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("update").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("No record found to update.");
            return false;
        }

        try {
            // Lấy record và chỉ kiểm tra region
            Record record = client.get(readPolicy, randomKey);
            String recordRegion = null;
            Map<String, Object> sub = null;
            if (record != null) {
                Object subObj = record.getValue("sub");
                if (subObj instanceof Map) {
                    sub = (Map<String, Object>) subObj;
                    recordRegion = (String) sub.get("r");
                }
            }
            if (!region.equals(recordRegion)) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Update lại dữ liệu bal - chỉ giữ các trường cần thiết
            Map<String, Map<String, Object>> bal = new HashMap<>();
            for (int i = 0; i < 2; i++) {
                Map<String, Object> balEntry = new HashMap<>();
                balEntry.put("i", random.nextLong());
                balEntry.put("g", random.nextLong());
                balEntry.put("c", random.nextLong());
                balEntry.put("r", random.nextLong());
                balEntry.put("t", random.nextLong());
                balEntry.put("e", System.currentTimeMillis());
                balEntry.put("x", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
                balEntry.put("s", 1);
                bal.put("b" + i, balEntry);
            }
            Bin balBin = new Bin("bal", bal);
            Bin subBin = sub != null ? new Bin("sub", sub) : null;

            // Tách trường lastUpdate ra thành bin riêng
            long lastUpdate = System.currentTimeMillis();
            Bin luBin = new Bin("lu", lastUpdate);

            // Thêm bin để kiểm soát kích cỡ dữ liệu (nhỏ hơn)
            int ctrlSize = ThreadLocalRandom.current().nextInt(5, 10);
            byte[] randomBytes = new byte[ctrlSize];
            ThreadLocalRandom.current().nextBytes(randomBytes);
            Bin ctrlBin = new Bin("ctrl", randomBytes);

            // Ghi lại cả bin sub và bal
            if (subBin != null) {
                client.put(writePolicy, randomKey, subBin, balBin, luBin, ctrlBin);
            } else {
                client.put(writePolicy, randomKey, balBin, luBin, ctrlBin);
            }

            if (!loggedUpdate) {
                Object keyObj = randomKey.userKey != null ? randomKey.userKey.getObject() : null;
                String keyStr = keyObj instanceof byte[] ? bytesToHex((byte[]) keyObj) : String.valueOf(keyObj);
                System.out.println("[UPDATE] Key: " + keyStr + " (Light version)");
                loggedUpdate = true;
            }
            return true;
        } catch (AerospikeException e) {
            System.err.println("Error updating record with key: " + randomKey.userKey + " (error: " + e.getMessage() + ")");
        } finally {
            randomKeys.offer(randomKey);
        }
        return false;
    }

    private static boolean performDelete(AerospikeClient client, String namespace, String setName,
            ConcurrentLinkedQueue<Key> randomKeys, String region) {
        // Kiểm tra giới hạn cho region và loại thao tác
        if (OPERATION_COUNTERS.get(region).get("delete").get() >= REGION_LIMITS.get(region)) {
            return false;
        }

        Key randomKey = randomKeys.poll();
        if (randomKey == null) {
            System.err.println("No record found to delete.");
            return false;
        }

        try {
            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(null, randomKey);
            if (record == null) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Kiểm tra region của record
            Object subObj = record.getValue("sub");
            if (!(subObj instanceof Map)) {
                randomKeys.offer(randomKey);
                return false;
            }
            Map<String, Object> sub = (Map<String, Object>) subObj;
            String recordRegion = (String) sub.get("r");
            if (!region.equals(recordRegion)) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Giữ lại toàn bộ bin sub, xoá các bin khác
            Bin subBin = new Bin("sub", sub);
            Bin balBin = Bin.asNull("bal");
            Bin acmBin = Bin.asNull("acm");
            Bin prdBin = Bin.asNull("prd");
            Bin chrBin = Bin.asNull("chr");
            Bin hisBin = Bin.asNull("his");
            Bin genBin = Bin.asNull("gen");

            // Tách trường lastUpdate ra thành bin riêng
            long lastUpdate = System.currentTimeMillis();
            Bin luBin = new Bin("lu", lastUpdate);

            // Thêm bin để kiểm soát kích cỡ dữ liệu (nhỏ hơn)
            int ctrlSize = ThreadLocalRandom.current().nextInt(5, 10);
            byte[] randomBytes = new byte[ctrlSize];
            ThreadLocalRandom.current().nextBytes(randomBytes);
            Bin ctrlBin = new Bin("ctrl", randomBytes);

            client.put(null, randomKey, subBin, balBin, acmBin, prdBin, chrBin, hisBin, genBin, luBin, ctrlBin);
            if (!loggedDelete) {
                Object keyObj = randomKey.userKey != null ? randomKey.userKey.getObject() : null;
                String keyStr = keyObj instanceof byte[] ? bytesToHex((byte[]) keyObj) : String.valueOf(keyObj);
                System.out.println("[DELETE] Key: " + keyStr + " (Light version)");
                loggedDelete = true;
            }
            return true;
        } catch (AerospikeException e) {
            System.err.println("Error deleting record with key: " + randomKey.userKey + " (error: " + e.getMessage() + ")");
        }
        return false;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static ConcurrentLinkedQueue<Key> getRandomKeysFromDatabase(AerospikeClient client, String namespace,
            String setName, int limit) {
        ConcurrentLinkedQueue<Key> keys = new ConcurrentLinkedQueue<>();
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.setMaxRecords(limit);

        Statement statement = new Statement();
        statement.setNamespace(namespace);
        statement.setSetName(setName);

        try (RecordSet recordSet = client.query(queryPolicy, statement)) {
            while (recordSet.next() && keys.size() < limit) {
                keys.add(recordSet.getKey());
            }
        } catch (AerospikeException e) {
            System.err.println("Error getting key list: " + e.getMessage());
        }

        return keys;
    }
} 