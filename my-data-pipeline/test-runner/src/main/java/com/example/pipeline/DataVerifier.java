package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DataVerifier {
    private static final Logger logger = LoggerFactory.getLogger(DataVerifier.class);
    private static final int SAMPLE_SIZE = 16547; // Số lượng key cần kiểm tra 16547
    private static final int SLEEP_INTERVAL = 1000; // Sleep sau moi 1000 record
    private static final int SLEEP_DURATION = 3000; // Sleep 3 giay

    private static AerospikeClient connectToAerospike(String host, int[] ports) {
        AerospikeException lastException = null;
        
        for (int port : ports) {
            try {
                logger.info("Thu ket noi den " + host + ":" + port);
                return new AerospikeClient(host, port);
            } catch (AerospikeException e) {
                lastException = e;
                logger.error("Khong the ket noi den " + host + ":" + port + " - " + e.getMessage());
            }
        }
        
        throw new AerospikeException("Khong the ket noi den Aerospike server. Da thu cac port: " + 
            String.join(", ", java.util.Arrays.stream(ports).mapToObj(String::valueOf).toArray(String[]::new)));
    }

    public static void main(String[] args) {
        try {
            // Load configuration
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Get producer (source) configuration
            Config.Producer producer = config.getProducers().get(0);
            String producerHost = producer.getHost();
            int producerPort = producer.getPort();
            String producerNamespace = producer.getNamespace();
            String producerSetName = producer.getSet();

            // Connect to source Aerospike
            logger.info("Ket noi den source DB...");
            AerospikeClient sourceClient = connectToAerospike(producerHost, new int[]{producerPort});
            
            // Connect to destination Aerospike
            logger.info("Ket noi den destination DB...");
            // Lấy host và port từ consumer đầu tiên làm destination
            Config.Consumer firstConsumer = config.getConsumers().get(0);
            String destHost = firstConsumer.getHost();
            int destPort = firstConsumer.getPort();
            AerospikeClient destClient = connectToAerospike(destHost, new int[]{destPort});

            QueryPolicy policy = new QueryPolicy();
            policy.sendKey = true;

            AtomicInteger totalVerified = new AtomicInteger(0);
            AtomicInteger mismatches = new AtomicInteger(0);
            AtomicInteger duplicates = new AtomicInteger(0);
            
            // Track results by region
            Map<String, RegionStats> regionStats = new HashMap<>();
            
            // Track duplicate keys
            Set<String> processedKeys = new HashSet<>();

            // Lay danh sach key tu DB1
            List<Key> randomKeys = getRandomKeys(sourceClient, producerNamespace, producerSetName, SAMPLE_SIZE);
            logger.info("Da lay " + randomKeys.size() + " key ngau nhien tu DB1");

            // Kiem tra tung key
            for (int i = 0; i < randomKeys.size(); i++) {
                Key sourceKey = randomKeys.get(i);
                String keyString = new String((byte[])sourceKey.userKey.getObject());
                
                // Check for duplicates
                if (processedKeys.contains(keyString)) {
                    duplicates.incrementAndGet();
                    logger.debug("Duplicate key found: " + keyString);
                    continue;
                }
                processedKeys.add(keyString);
                
                // Sleep sau moi SLEEP_INTERVAL record
                if (i > 0 && i % SLEEP_INTERVAL == 0) {
                    logger.info("Da kiem tra " + i + " record, tam dung " + (SLEEP_DURATION/1000) + " giay...");
                    Thread.sleep(SLEEP_DURATION);
                }

                // Doc record tu DB1
                com.aerospike.client.Record sourceRecord = sourceClient.get(policy, sourceKey);
                if (sourceRecord == null) {
                    logger.debug("[SKIP] sourceRecord null for key: " + keyString);
                    continue;
                }

                // Lấy bin 'sub' từ sourceRecord
                Object sourceSubObj = sourceRecord.getValue("sub");
                if (!(sourceSubObj instanceof Map)) {
                    logger.debug("[SKIP] bin 'sub' null hoặc không phải Map cho key: " + keyString);
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> sourceSub = (Map<String, Object>) sourceSubObj;
                // Lấy region từ trường 'r' trong sub
                String region = (String) sourceSub.get("r");
                if (region == null) {
                    logger.debug("[SKIP] region (sub.r) null for key: " + keyString);
                    continue;
                }

                // Initialize region stats if not exists
                regionStats.putIfAbsent(region, new RegionStats());

                // Lay consumer tuong ung voi region
                List<String> consumerNames = config.getConsumersForRegion(region);
                if (consumerNames == null || consumerNames.isEmpty()) {
                    logger.debug("[SKIP] No consumerNames for region: " + region + ", key: " + keyString);
                    continue;
                }

                String consumerName = consumerNames.get(0);
                Config.Consumer consumer = config.getConsumers().stream()
                    .filter(c -> c.getName().equals(consumerName))
                    .findFirst()
                    .orElse(null);

                if (consumer == null) {
                    logger.debug("[SKIP] No consumer config for: " + consumerName + ", key: " + keyString);
                    continue;
                }

                // Doc record tu DB dich
                Key destKey = new Key(consumer.getNamespace(), consumer.getSet(), sourceKey.userKey);
                com.aerospike.client.Record destRecord = destClient.get(policy, destKey);

                RegionStats stats = regionStats.get(region);
                stats.totalVerified++;

                if (destRecord == null) {
                    logger.debug("Mismatch: Key " + keyString + " khong ton tai trong DB dich (region: " + region + ", namespace: " + consumer.getNamespace() + ")");
                    mismatches.incrementAndGet();
                    stats.mismatches++;
                } else {
                    // Lấy bin 'sub' từ cả hai record
                    Object destSubObj = destRecord.getValue("sub");
                    if (!(destSubObj instanceof Map)) {
                        logger.debug("Mismatch: Key " + keyString + ", bin 'sub' khong phai Map (DB1: " + (sourceSubObj == null ? "null" : sourceSubObj.getClass()) + ", DB2: " + (destSubObj == null ? "null" : destSubObj.getClass()) + ")");
                        mismatches.incrementAndGet();
                        stats.mismatches++;
                    } else {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> destSub = (Map<String, Object>) destSubObj;
                        // Các trường cần kiểm tra
                        String[] fields = {"m", "si", "ci", "ct", "rt", "ss", "st"};
                        // Chọn ngẫu nhiên 3 trường để kiểm tra
                        java.util.List<String> fieldList = java.util.Arrays.asList(fields);
                        java.util.Collections.shuffle(fieldList);
                        java.util.List<String> randomFields = fieldList.subList(0, 3);
                        for (String field : randomFields) {
                            Object srcVal = sourceSub.get(field);
                            Object dstVal = destSub.get(field);
                            if (srcVal == null && dstVal == null) {
                                logger.debug("[INFO] Key " + keyString + ", field '" + field + "' both null");
                                continue;
                            }
                            if (srcVal == null || dstVal == null || !srcVal.equals(dstVal)) {
                                logger.debug("Mismatch: Key " + keyString + ", field '" + field + "' differs (DB1: " + srcVal + ", DB2: " + dstVal + ")");
                                mismatches.incrementAndGet();
                                stats.mismatches++;
                            } else {
                                logger.debug("[OK] Key " + keyString + ", field '" + field + "' match: " + srcVal);
                            }
                        }
                    }
                }

                totalVerified.incrementAndGet();
            }

            // In ket qua tong hop
            logger.info("\n=== Verification Results ===");
            logger.info("Total records verified: " + totalVerified.get());
            logger.info("Total mismatches found: " + mismatches.get());
            logger.info("Total duplicates found: " + duplicates.get());
            logger.info("Overall verification accuracy: " + 
                String.format("%.2f%%", (totalVerified.get() - mismatches.get()) * 100.0 / totalVerified.get()));

            // In ket qua theo tung region
            logger.info("\n=== Results by Region ===");
            for (Map.Entry<String, RegionStats> entry : regionStats.entrySet()) {
                String region = entry.getKey();
                RegionStats stats = entry.getValue();
                double accuracy = (stats.totalVerified - stats.mismatches) * 100.0 / stats.totalVerified;
                logger.info("Region: " + region);
                logger.info("  Total verified: " + stats.totalVerified);
                logger.info("  Mismatches: " + stats.mismatches);
                logger.info("  Accuracy: " + String.format("%.2f%%", accuracy));
                logger.info("");
            }

            // Close connections
            sourceClient.close();
            destClient.close();

        } catch (Exception e) {
            logger.error("Error during verification: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static class RegionStats {
        int totalVerified = 0;
        int mismatches = 0;
    }

    private static List<Key> getRandomKeys(AerospikeClient client, String namespace, String setName, int sampleSize) {
        List<Key> keys = new ArrayList<>();
        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.sendKey = true;
        final int MAX_KEYS = 20000;

        try {
            // Scan de lay toi da 20,000 key
            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                if (keys.size() < MAX_KEYS) {
                    keys.add(key);
                }
            });

            // Lay ngau nhien sampleSize key
            if (keys.size() > sampleSize) {
                Random random = new Random();
                List<Key> sampledKeys = new ArrayList<>();
                for (int i = 0; i < sampleSize; i++) {
                    int randomIndex = random.nextInt(keys.size());
                    sampledKeys.add(keys.get(randomIndex));
                    keys.remove(randomIndex);
                }
                return sampledKeys;
            }
        } catch (Exception e) {
            logger.error("Error getting random keys: " + e.getMessage());
        }

        return keys;
    }
} 