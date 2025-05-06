package com.test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import io.github.cdimascio.dotenv.Dotenv;

import java.util.concurrent.atomic.AtomicInteger;

public class TestAerospikeCheck {
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

            System.out.println("Checking Aerospike status...");
            System.out.println("Host: " + destinationHost);
            System.out.println("Port: " + destinationPort);
            System.out.println("Namespace 096: " + consumerNamespace096);
            System.out.println("Namespace 033: " + consumerNamespace033);
            System.out.println("Set Name: " + consumerSetName);

            // Initialize Aerospike client
            AerospikeClient client = new AerospikeClient(destinationHost, destinationPort);

            // Check connection
            if (!client.isConnected()) {
                System.err.println("Failed to connect to Aerospike");
                return;
            }
            System.out.println("\nSuccessfully connected to Aerospike");

            // Check namespaces
            System.out.println("\nChecking namespaces...");
            checkNamespace(client, consumerNamespace096, consumerSetName);
            checkNamespace(client, consumerNamespace033, consumerSetName);

            // Check data
            System.out.println("\nChecking data...");
            checkData(client, consumerNamespace096, consumerSetName);
            checkData(client, consumerNamespace033, consumerSetName);

            // Close client
            client.close();

        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void checkNamespace(AerospikeClient client, String namespace, String setName) {
        try {
            System.out.println("\nChecking namespace: " + namespace);
            
            // Check if namespace exists
            if (!client.isConnected()) {
                System.err.println("Client not connected");
                return;
            }

            // Try to read a test record
            Key testKey = new Key(namespace, setName, "test_key");
            Record record = client.get(null, testKey);
            
            if (record != null) {
                System.out.println("Namespace " + namespace + " is accessible");
            } else {
                System.out.println("Namespace " + namespace + " exists but no test record found");
            }

            // Try to write a test record
            try {
                client.put(null, testKey, new com.aerospike.client.Bin("test", "test"));
                System.out.println("Successfully wrote test record to " + namespace);
                
                // Verify write
                record = client.get(null, testKey);
                if (record != null) {
                    System.out.println("Successfully verified test record in " + namespace);
                }
                
                // Clean up test record
                client.delete(null, testKey);
                System.out.println("Successfully cleaned up test record from " + namespace);
            } catch (Exception e) {
                System.err.println("Error writing test record to " + namespace + ": " + e.getMessage());
            }

        } catch (Exception e) {
            System.err.println("Error checking namespace " + namespace + ": " + e.getMessage());
        }
    }

    private static void checkData(AerospikeClient client, String namespace, String setName) {
        try {
            System.out.println("\nChecking data in namespace: " + namespace);
            
            // Try to read some records
            AtomicInteger count = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            // Try to read first 5 records
            for (int i = 0; i < 5; i++) {
                String key = String.format("%09d", i);
                Key recordKey = new Key(namespace, setName, key.getBytes());
                
                try {
                    Record record = client.get(null, recordKey);
                    if (record != null) {
                        count.incrementAndGet();
                        System.out.printf("Found record %s in %s%n", key, namespace);
                        System.out.println("Record bins: " + record.bins);
                    } else {
                        System.out.printf("Record %s not found in %s%n", key, namespace);
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.printf("Error reading record %s: %s%n", key, e.getMessage());
                }
            }

            System.out.printf("\nFound %d records in %s%n", count.get(), namespace);
            if (errorCount.get() > 0) {
                System.err.printf("Encountered %d errors while reading records%n", errorCount.get());
            }

        } catch (Exception e) {
            System.err.println("Error checking data in " + namespace + ": " + e.getMessage());
        }
    }
} 