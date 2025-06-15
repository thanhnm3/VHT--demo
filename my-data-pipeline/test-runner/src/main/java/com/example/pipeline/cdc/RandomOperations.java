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

public class RandomOperations {
    private static final String[] SERVICE_TYPES = {
        "MOBILE", "FIXED", "BROADBAND"
    };

    private static final String[] REGIONS = {
        "north", "central", "south"
    };

    private static final int MAX_RECORDS_PER_REGION = 200_000;
    private static final int KEY_LIMIT = 30_000;
    private static final int OPERATIONS_PER_SECOND_PER_REGION = 100;
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
                        }
                        break;
                    case 1: // Update
                        operationPerformed = performUpdate(client, writePolicy, readPolicy, namespace, setName, randomKeys, random, region, config);
                        if (operationPerformed) {
                            totalUpdateCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("update").incrementAndGet();
                        }
                        break;
                    case 2: // Delete
                        operationPerformed = performDelete(client, namespace, setName, randomKeys, region);
                        if (operationPerformed) {
                            totalDeleteCount.incrementAndGet();
                            OPERATION_COUNTERS.get(region).get("delete").incrementAndGet();
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
        
        // Random service type
        String serviceType = SERVICE_TYPES[random.nextInt(SERVICE_TYPES.length)];
        
        // Random province from this region
        List<String> provinces = config.getRegion_groups().getProvincesByRegion(region);
        String province = provinces.get(random.nextInt(provinces.size()));

        // Generate random notes
        byte[] notes = generateRandomBytes(100, 1_000);

        // Create bins with natural data types
        Bin userIdBin = new Bin("user_id", userId);
        Bin phoneBin = new Bin("phone", phoneNumber);
        Bin serviceTypeBin = new Bin("service_type", serviceType);
        Bin provinceBin = new Bin("province", province);
        Bin regionBin = new Bin("region", region);
        Bin lastUpdateBin = new Bin("last_updated", System.currentTimeMillis());
        Bin notesBin = new Bin("notes", notes);

        try {
            client.put(writePolicy, key, userIdBin, phoneBin, serviceTypeBin, 
                      provinceBin, regionBin, lastUpdateBin, notesBin);
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
            // Kiểm tra xem bản ghi có tồn tại không
            Record record = client.get(readPolicy, randomKey);
            if (record == null) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Kiểm tra region của record
            String recordRegion = (String) record.getValue("region");
            if (!region.equals(recordRegion)) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Random service type
            String serviceType = SERVICE_TYPES[random.nextInt(SERVICE_TYPES.length)];
            
            // Random province from this region
            List<String> provinces = config.getRegion_groups().getProvincesByRegion(region);
            String province = provinces.get(random.nextInt(provinces.size()));

            // Generate new random notes
            byte[] notes = generateRandomBytes(100, 1_000);

            // Update bins
            Bin serviceTypeBin = new Bin("service_type", serviceType);
            Bin provinceBin = new Bin("province", province);
            Bin lastUpdateBin = new Bin("last_updated", System.currentTimeMillis());
            Bin notesBin = new Bin("notes", notes);

            client.put(writePolicy, randomKey, serviceTypeBin, provinceBin, lastUpdateBin, notesBin);
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
            String recordRegion = (String) record.getValue("region");
            if (!region.equals(recordRegion)) {
                randomKeys.offer(randomKey);
                return false;
            }

            // Set all bins to null except region and last_updated
            Bin userIdBin = Bin.asNull("user_id");
            Bin phoneBin = Bin.asNull("phone");
            Bin serviceTypeBin = Bin.asNull("service_type");
            Bin provinceBin = Bin.asNull("province");
            Bin regionBin = new Bin("region", region);  // Giữ lại region
            Bin lastUpdateBin = new Bin("last_updated", System.currentTimeMillis());
            Bin notesBin = Bin.asNull("notes");

            client.put(null, randomKey, userIdBin, phoneBin, serviceTypeBin, 
                      provinceBin, regionBin, lastUpdateBin, notesBin);
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
