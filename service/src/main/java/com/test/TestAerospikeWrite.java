package com.test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import io.github.cdimascio.dotenv.Dotenv;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestAerospikeWrite {
    private static final int NUM_RECORDS = 1000;
    private static final int NUM_THREADS = 4;
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger errorCount = new AtomicInteger(0);

    private static boolean testConnection(AerospikeClient client, String namespace, String setName) {
        try {
            if (client != null && client.isConnected()) {
                // Test write to verify connection using an existing namespace
                Key testKey = new Key(namespace, setName, "test_connection");
                Bin testBin = new Bin("test", "test");
                client.put(null, testKey, testBin);
                client.delete(null, testKey);
                System.out.println("Successfully connected to Aerospike");
                return true;
            }
        } catch (Exception e) {
            System.err.println("Connection test failed: " + e.getMessage());
        }
        return false;
    }

    public static void main(String[] args) {
        try {
            // Load configuration from .env
            Dotenv dotenv = Dotenv.configure()
                    .directory("service/.env")
                    .load();

            // Read environment variables
            String destinationHost = dotenv.get("AEROSPIKE_CONSUMER_HOST");
            int destinationPort = Integer.parseInt(dotenv.get("AEROSPIKE_CONSUMER_PORT"));
            String consumerNamespace096 = dotenv.get("CONSUMER_NAMESPACE_096");
            String consumerNamespace033 = dotenv.get("CONSUMER_NAMESPACE_033");
            String consumerSetName = dotenv.get("CONSUMER_SET_NAME");

            System.out.println("Configuration loaded:");
            System.out.println("Host: " + destinationHost);
            System.out.println("Port: " + destinationPort);
            System.out.println("Namespace 096: " + consumerNamespace096);
            System.out.println("Namespace 033: " + consumerNamespace033);
            System.out.println("Set Name: " + consumerSetName);

            // Initialize Aerospike client with retry policy
            AerospikeClient client = new AerospikeClient(destinationHost, destinationPort);
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.totalTimeout = 10000; // 10 seconds timeout
            writePolicy.maxRetries = 5;
            writePolicy.sleepBetweenRetries = 1000;
            writePolicy.sendKey = true;

            // Test connection using consumer_096 namespace
            System.out.println("\nTesting connection to Aerospike...");
            if (!testConnection(client, consumerNamespace096, consumerSetName)) {
                System.err.println("Failed to connect to Aerospike. Exiting...");
                return;
            }

            System.out.println("\nStarting test write to Aerospike...");
            System.out.println("Number of records to write: " + NUM_RECORDS);
            System.out.println("Number of threads: " + NUM_THREADS);

            // Create thread pool
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

            // Submit write tasks
            for (int i = 0; i < NUM_RECORDS; i++) {
                final int recordNum = i;
                executor.submit(() -> {
                    try {
                        // Generate random data
                        String key = String.format("%09d", recordNum);
                        String prefix = recordNum % 2 == 0 ? "096" : "033";
                        String namespace = prefix.equals("096") ? consumerNamespace096 : consumerNamespace033;
                        
                        // Create test data
                        byte[] testData = generateRandomData();
                        long timestamp = System.currentTimeMillis();

                        // Create key and bins
                        Key recordKey = new Key(namespace, consumerSetName, key.getBytes());
                        Bin dataBin = new Bin("testData", testData);
                        Bin timestampBin = new Bin("timestamp", timestamp);
                        Bin prefixBin = new Bin("prefix", prefix);

                        System.out.printf("Writing record %s to namespace %s%n", key, namespace);

                        // Write to Aerospike with retry
                        int retries = 3;
                        boolean writeSuccess = false;
                        while (!writeSuccess && retries > 0) {
                            try {
                                client.put(writePolicy, recordKey, dataBin, timestampBin, prefixBin);
                                writeSuccess = true;
                            } catch (Exception e) {
                                retries--;
                                if (retries > 0) {
                                    System.err.printf("Retry writing record %s (attempts remaining: %d): %s%n", 
                                        key, retries, e.getMessage());
                                    Thread.sleep(1000);
                                } else {
                                    throw e;
                                }
                            }
                        }
                        
                        // Verify write
                        if (client.exists(null, recordKey)) {
                            successCount.incrementAndGet();
                            if (successCount.get() % 100 == 0) {
                                System.out.printf("Successfully wrote %d records%n", successCount.get());
                            }
                        } else {
                            errorCount.incrementAndGet();
                            System.err.printf("Failed to verify write for record %s%n", key);
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        System.err.printf("Error writing record %d: %s%n", recordNum, e.getMessage());
                        e.printStackTrace();
                    }
                });
            }

            // Shutdown executor and wait for completion
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("Timeout waiting for write operations to complete");
                executor.shutdownNow();
            }

            // Print final statistics
            System.out.println("\nTest completed:");
            System.out.printf("Total records: %d%n", NUM_RECORDS);
            System.out.printf("Successful writes: %d%n", successCount.get());
            System.out.printf("Failed writes: %d%n", errorCount.get());

            // Verify final data
            System.out.println("\nVerifying data in namespaces:");
            verifyNamespaceData(client, consumerNamespace096, consumerSetName);
            verifyNamespaceData(client, consumerNamespace033, consumerSetName);

            // Close Aerospike client
            client.close();

        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void verifyNamespaceData(AerospikeClient client, String namespace, String setName) {
        try {
            // Try to read a few records
            for (int i = 0; i < 5; i++) {
                String key = String.format("%09d", i);
                Key recordKey = new Key(namespace, setName, key.getBytes());
                if (client.exists(null, recordKey)) {
                    System.out.printf("Found record %s in namespace %s%n", key, namespace);
                } else {
                    System.out.printf("Record %s not found in namespace %s%n", key, namespace);
                }
            }
        } catch (Exception e) {
            System.err.printf("Error verifying namespace %s: %s%n", namespace, e.getMessage());
        }
    }

    private static byte[] generateRandomData() {
        Random random = new Random();
        int size = random.nextInt(900) + 100; // Random size between 100 and 1000 bytes
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }
} 