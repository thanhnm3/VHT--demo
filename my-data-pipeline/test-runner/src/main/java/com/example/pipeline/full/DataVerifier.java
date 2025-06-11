package com.example.pipeline.full;

import com.aerospike.client.*;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.HashMap;

public class DataVerifier {
    private static final int SAMPLE_SIZE = 16547; // Số lượng key cần kiểm tra
    private static final int[] POSSIBLE_PORTS = {4000, 4001, 4002, 4003, 4004};
    private static final int SLEEP_INTERVAL = 1000; // Sleep sau moi 1000 record
    private static final int SLEEP_DURATION = 3000; // Sleep 3 giay

    private static AerospikeClient connectToAerospike(String host, int[] ports) {
        AerospikeException lastException = null;
        
        for (int port : ports) {
            try {
                System.out.println("Thu ket noi den " + host + ":" + port);
                return new AerospikeClient(host, port);
            } catch (AerospikeException e) {
                lastException = e;
                System.out.println("Khong the ket noi den " + host + ":" + port + " - " + e.getMessage());
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
            String producerHost = "localhost";
            int producerPort = 3000;
            String producerNamespace = producer.getNamespace();
            String producerSetName = producer.getSet();

            // Connect to source Aerospike
            System.out.println("Ket noi den source DB...");
            AerospikeClient sourceClient = connectToAerospike(producerHost, new int[]{producerPort});
            
            // Connect to destination Aerospike
            System.out.println("Ket noi den destination DB...");
            AerospikeClient destClient = connectToAerospike("localhost", POSSIBLE_PORTS);

            QueryPolicy policy = new QueryPolicy();
            policy.sendKey = true;

            AtomicInteger totalVerified = new AtomicInteger(0);
            AtomicInteger mismatches = new AtomicInteger(0);
            
            // Track results by region
            Map<String, RegionStats> regionStats = new HashMap<>();

            // Lay danh sach key tu DB1
            List<Key> randomKeys = getRandomKeys(sourceClient, producerNamespace, producerSetName, SAMPLE_SIZE);
            System.out.println("Da lay " + randomKeys.size() + " key ngau nhien tu DB1");

            // Kiem tra tung key
            for (int i = 0; i < randomKeys.size(); i++) {
                Key sourceKey = randomKeys.get(i);
                
                // Sleep sau moi SLEEP_INTERVAL record
                if (i > 0 && i % SLEEP_INTERVAL == 0) {
                    System.out.println("Da kiem tra " + i + " record, tam dung " + (SLEEP_DURATION/1000) + " giay...");
                    Thread.sleep(SLEEP_DURATION);
                }

                // Doc record tu DB1
                com.aerospike.client.Record sourceRecord = sourceClient.get(policy, sourceKey);
                if (sourceRecord == null) {
                    continue;
                }

                // Lay thong tin region tu record
                String region = (String) sourceRecord.getValue("region");
                if (region == null) {
                    continue;
                }

                // Initialize region stats if not exists
                regionStats.putIfAbsent(region, new RegionStats());

                // Lay consumer tuong ung voi region
                List<String> consumerNames = config.getConsumersForRegion(region);
                if (consumerNames == null || consumerNames.isEmpty()) {
                    continue;
                }

                String consumerName = consumerNames.get(0);
                Config.Consumer consumer = config.getConsumers().stream()
                    .filter(c -> c.getName().equals(consumerName))
                    .findFirst()
                    .orElse(null);

                if (consumer == null) {
                    continue;
                }

                // Doc record tu DB dich
                Key destKey = new Key(consumer.getNamespace(), consumer.getSet(), sourceKey.userKey);
                com.aerospike.client.Record destRecord = destClient.get(policy, destKey);

                RegionStats stats = regionStats.get(region);
                stats.totalVerified++;

                if (destRecord == null) {
                    System.out.printf("Mismatch: Key %s khong ton tai trong DB dich (region: %s, namespace: %s)%n",
                        new String((byte[])sourceKey.userKey.getObject()), region, consumer.getNamespace());
                    mismatches.incrementAndGet();
                    stats.mismatches++;
                } else {
                    // Kiem tra cac truong
                    String sourceProvince = (String) sourceRecord.getValue("province");
                    String destProvince = (String) destRecord.getValue("province");

                    if (sourceProvince != null && destProvince != null && !sourceProvince.equals(destProvince)) {
                        System.out.printf("Mismatch: Key %s co province khac nhau (DB1: %s, DB2: %s, region: %s)%n",
                            new String((byte[])sourceKey.userKey.getObject()), sourceProvince, destProvince, region);
                        mismatches.incrementAndGet();
                        stats.mismatches++;
                    }
                }

                totalVerified.incrementAndGet();
            }

            // In ket qua tong hop
            System.out.println("\n=== Verification Results ===");
            System.out.println("Total records verified: " + totalVerified.get());
            System.out.println("Total mismatches found: " + mismatches.get());
            System.out.println("Overall verification accuracy: " + 
                String.format("%.2f%%", (totalVerified.get() - mismatches.get()) * 100.0 / totalVerified.get()));

            // In ket qua theo tung region
            System.out.println("\n=== Results by Region ===");
            for (Map.Entry<String, RegionStats> entry : regionStats.entrySet()) {
                String region = entry.getKey();
                RegionStats stats = entry.getValue();
                double accuracy = (stats.totalVerified - stats.mismatches) * 100.0 / stats.totalVerified;
                System.out.printf("Region: %s%n", region);
                System.out.printf("  Total verified: %d%n", stats.totalVerified);
                System.out.printf("  Mismatches: %d%n", stats.mismatches);
                System.out.printf("  Accuracy: %.2f%%%n", accuracy);
                System.out.println();
            }

            // Close connections
            sourceClient.close();
            destClient.close();

        } catch (Exception e) {
            System.err.println("Error during verification: " + e.getMessage());
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
            System.err.println("Error getting random keys: " + e.getMessage());
        }

        return keys;
    }
} 