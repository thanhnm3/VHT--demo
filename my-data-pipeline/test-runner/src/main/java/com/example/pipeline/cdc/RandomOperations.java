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

public class RandomOperations {
    private static final String[] SERVICE_TYPES = {
        "MOBILE", "FIXED", "BROADBAND"
    };

    private static final String[] REGIONS = {
        "north", "central", "south"
    };


    private static final int KEY_LIMIT = 40_000;
    private static final int OPERATIONS_PER_SECOND_PER_REGION = 500;
    private static final Map<String, Integer> REGION_LIMITS = Map.of(
        "north", 1_000,
        "central", 1_000,
        "south", 1_000
    );
    private static final Map<String, Map<String, AtomicInteger>> OPERATION_COUNTERS = new HashMap<>();
    private static final Map<String, RateLimiter> REGION_RATE_LIMITERS = new HashMap<>();

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
                System.out.println("[RandomOps] Miền " + region + ": Insert=" + ins + ", Update=" + upd + ", Delete=" + del);
            }
            System.out.println("[RandomOps] Đã thực hiện: " + totalOps + " / " + totalMaxOps);
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

        System.out.println("\n=== Operation Results ===");
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
        System.out.println("================================");

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

        // Build sub map giống RandomInsert
        Map<String, Object> sub = new HashMap<>();
        sub.put("m", phoneNumber);
        sub.put("si", userId);
        sub.put("ci", UUID.randomUUID().toString());
        sub.put("df", false);
        sub.put("st", random.nextInt(5));
        sub.put("ss", "ACTIVE");
        sub.put("pc", random.nextLong());
        sub.put("bl", List.of(random.nextLong(), random.nextLong()));
        sub.put("al", List.of(random.nextLong(), random.nextLong()));
        sub.put("pl", List.of(random.nextLong(), random.nextLong()));
        sub.put("gl", List.of(random.nextLong(), random.nextLong()));
        sub.put("ml", List.of(random.nextLong(), random.nextLong()));
        sub.put("vl", List.of("VPN1", "VPN2"));
        sub.put("ms", Map.of(1, "LAST_SESSION"));
        sub.put("mp", random.nextLong());
        sub.put("cl", List.of("CELL1", "CELL2"));
        sub.put("li", random.nextInt(3));
        sub.put("r", region);
        sub.put("lu", System.currentTimeMillis());
        sub.put("im", "IMSI" + random.nextInt(1000000));
        sub.put("ic", "ICCID" + random.nextInt(1000000));
        sub.put("pw", "PASS" + random.nextInt(1000000));
        sub.put("bs", "BCCS" + random.nextInt(1000000));
        sub.put("bc", "CUST" + random.nextInt(1000000));
        sub.put("rt", "PREPAID");
        sub.put("sc", "INDIVIDUAL");
        sub.put("ct", "CONT" + random.nextInt(1000000));
        sub.put("ct", "RESIDENTIAL");
        sub.put("cv", "NORMAL");
        sub.put("zl", List.of("ZONE1", "ZONE2"));
        sub.put("p", province);
        sub.put("email", "user" + random.nextInt(1000000) + "@example.com");
        sub.put("address", "Address " + random.nextInt(1000));
        sub.put("firstName", "First" + random.nextInt(1000));
        sub.put("lastName", "Last" + random.nextInt(1000));
        sub.put("completeDate", System.currentTimeMillis());
        sub.put("birthDay", System.currentTimeMillis() - (random.nextInt(365) * 24 * 60 * 60 * 1000L));
        sub.put("startNotifyTime", System.currentTimeMillis());
        sub.put("endNotifyTime", System.currentTimeMillis() + (random.nextInt(365) * 24 * 60 * 60 * 1000L));
        sub.put("countTopupTotal", random.nextLong());
        sub.put("countTopupFailure", random.nextLong());
        sub.put("countTopupSuccess", random.nextLong());
        sub.put("sex", random.nextInt(2));
        sub.put("effDate", System.currentTimeMillis());
        sub.put("expDate", System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L));
        sub.put("updateDate", System.currentTimeMillis());
        sub.put("state", 1);
        sub.put("charIdList", List.of(random.nextLong(), random.nextLong()));
        sub.put("level", random.nextInt(5));
        sub.put("of", random.nextLong());

        Bin subBin = new Bin("sub", sub);
        try {
            client.put(writePolicy, key, subBin);
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
            if (record != null) {
                Object subObj = record.getValue("sub");
                if (subObj instanceof Map) {
                    Map<String, Object> sub = (Map<String, Object>) subObj;
                    recordRegion = (String) sub.get("r");
                }
            }
            if (!region.equals(recordRegion)) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Random service type
            String serviceType = SERVICE_TYPES[random.nextInt(SERVICE_TYPES.length)];
            // Random province from this region
            List<String> provinces = config.getRegion_groups().getProvincesByRegion(region);
            String province = provinces.get(random.nextInt(provinces.size()));

            // Update trường trong sub
            if (record != null && record.getValue("sub") instanceof Map) {
                Map<String, Object> sub = (Map<String, Object>) record.getValue("sub");
                sub.put("st", random.nextInt(5));
                sub.put("p", province);
                sub.put("lu", System.currentTimeMillis());
                Bin subBin = new Bin("sub", sub);
                client.put(writePolicy, randomKey, subBin);
                return true;
            }
            return false;
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

            client.put(null, randomKey, subBin, balBin, acmBin, prdBin, chrBin, hisBin, genBin);
            return true;
        } catch (AerospikeException e) {
            System.err.println("Error deleting record with key: " + randomKey.userKey + " (error: " + e.getMessage() + ")");
        }
        return false;
    }

    private static byte[] generateRandomBytes(int minSize, int maxSize) {
        int size = ThreadLocalRandom.current().nextInt(minSize, maxSize + 1);
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
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
