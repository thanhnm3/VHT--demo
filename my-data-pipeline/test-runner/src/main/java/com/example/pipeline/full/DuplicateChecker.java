package com.example.pipeline.full;

import com.aerospike.client.*;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DuplicateChecker {
    private static final int[] POSSIBLE_PORTS = {4000, 4001, 4002, 4003, 4004};

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

            // Connect to destination Aerospike
            System.out.println("Ket noi den destination DB...");
            AerospikeClient destClient = connectToAerospike("localhost", POSSIBLE_PORTS);

            QueryPolicy policy = new QueryPolicy();
            policy.sendKey = true;

            // Check each consumer namespace
            for (Config.Consumer consumer : config.getConsumers()) {
                System.out.println("\n=== Checking namespace: " + consumer.getNamespace() + " ===");
                checkNamespaceForDuplicates(destClient, consumer.getNamespace(), consumer.getSet(), policy);
            }

            destClient.close();

        } catch (Exception e) {
            System.err.println("Error during duplicate check: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void checkNamespaceForDuplicates(AerospikeClient client, String namespace, String setName, QueryPolicy policy) {
        try {
            Set<String> allKeys = new HashSet<>();
            Set<String> duplicateKeys = new HashSet<>();
            AtomicLong totalRecords = new AtomicLong(0);
            AtomicLong duplicateCount = new AtomicLong(0);

            // Scan all records in the namespace
            ScanPolicy scanPolicy = new ScanPolicy();
            scanPolicy.sendKey = true;
            scanPolicy.concurrentNodes = false;
            scanPolicy.maxConcurrentNodes = 1;

            client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
                String keyString = new String((byte[])key.userKey.getObject());
                totalRecords.incrementAndGet();

                if (allKeys.contains(keyString)) {
                    duplicateKeys.add(keyString);
                    duplicateCount.incrementAndGet();
                    System.out.printf("Duplicate found: %s (namespace: %s, set: %s)%n", 
                                    keyString, namespace, setName);
                } else {
                    allKeys.add(keyString);
                }

                // Log progress every 10000 records
                if (totalRecords.get() % 10000 == 0) {
                    System.out.printf("Processed %d records, found %d duplicates...%n", 
                                    totalRecords.get(), duplicateCount.get());
                }
            });

            System.out.printf("Namespace: %s, Set: %s%n", namespace, setName);
            System.out.printf("Total records: %d%n", totalRecords.get());
            System.out.printf("Unique keys: %d%n", allKeys.size());
            System.out.printf("Duplicate keys: %d%n", duplicateCount.get());
            System.out.printf("Duplicate percentage: %.2f%%%n", 
                            (duplicateCount.get() * 100.0) / totalRecords.get());

            // Show some example duplicates
            if (!duplicateKeys.isEmpty()) {
                System.out.println("Example duplicate keys:");
                int count = 0;
                for (String dupKey : duplicateKeys) {
                    if (count++ < 10) { // Show first 10 duplicates
                        System.out.printf("  - %s%n", dupKey);
                    } else {
                        break;
                    }
                }
                if (duplicateKeys.size() > 10) {
                    System.out.printf("  ... and %d more%n", duplicateKeys.size() - 10);
                }
            }

        } catch (Exception e) {
            System.err.printf("Error checking namespace %s: %s%n", namespace, e.getMessage());
        }
    }
} 