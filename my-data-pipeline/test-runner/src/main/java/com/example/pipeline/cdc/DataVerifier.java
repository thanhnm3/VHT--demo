package com.example.pipeline.cdc;

import com.aerospike.client.*;
import com.aerospike.client.policy.QueryPolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.List;

public class DataVerifier {
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
            AerospikeClient sourceClient = new AerospikeClient(producerHost, producerPort);

            QueryPolicy policy = new QueryPolicy();
            policy.sendKey = true;

            AtomicInteger totalVerified = new AtomicInteger(0);
            AtomicInteger mismatches = new AtomicInteger(0);

            // Verify records for each region mapping
            for (Map.Entry<String, List<String>> entry : config.getRegion_mapping().entrySet()) {
                String region = entry.getKey();
                List<String> consumerNames = entry.getValue();
                
                // Get the first consumer for this region
                String consumerName = consumerNames.get(0);
                Config.Consumer consumer = config.getConsumers().stream()
                    .filter(c -> c.getName().equals(consumerName))
                    .findFirst()
                    .orElse(null);

                if (consumer == null) {
                    System.err.println("No consumer found for region: " + region);
                    continue;
                }

                // Connect to destination Aerospike for this region
                AerospikeClient destClient = new AerospikeClient(consumer.getHost(), consumer.getPort());
                
                for (int i = 1; i <= 1000; i++) {
                    String phoneNumber = String.format("%s%07d", region, i);
                    byte[] phoneBytes = phoneNumber.getBytes();

                    // Read from source
                    Key sourceKey = new Key(producerNamespace, producerSetName, phoneBytes);
                    com.aerospike.client.Record sourceRecord = sourceClient.get(policy, sourceKey);

                    // Read from destination
                    Key destKey = new Key(consumer.getNamespace(), consumer.getSet(), phoneBytes);
                    com.aerospike.client.Record destRecord = destClient.get(policy, destKey);

                    if (sourceRecord == null || destRecord == null) {
                        mismatches.incrementAndGet();
                        continue;
                    }

                    // Verify all fields
                    boolean isMatch = true;
                    
                    // Check basic fields
                    String[] fieldsToCheck = {"user_id", "phone", "service_type", "province", "region"};
                    for (String field : fieldsToCheck) {
                        Object sourceValue = sourceRecord.getValue(field);
                        Object destValue = destRecord.getValue(field);
                        if (!equals(sourceValue, destValue)) {
                            isMatch = false;
                            System.err.println(String.format("Mismatch in field %s for phone %s", field, phoneNumber));
                            break;
                        }
                    }

                    // Check last_updated
                    if (isMatch) {
                        Object sourceLastUpdate = sourceRecord.getValue("last_updated");
                        Object destLastUpdate = destRecord.getValue("last_updated");
                        if (!equals(sourceLastUpdate, destLastUpdate)) {
                            isMatch = false;
                            System.err.println(String.format("Mismatch in last_updated for phone %s", phoneNumber));
                        }
                    }

                    // Check notes (binary data)
                    if (isMatch) {
                        byte[] sourceNotes = (byte[]) sourceRecord.getValue("notes");
                        byte[] destNotes = (byte[]) destRecord.getValue("notes");
                        if (!Arrays.equals(sourceNotes, destNotes)) {
                            isMatch = false;
                            System.err.println(String.format("Mismatch in notes for phone %s", phoneNumber));
                        }
                    }

                    if (!isMatch) {
                        mismatches.incrementAndGet();
                    }

                    totalVerified.incrementAndGet();
                }

                // Close destination client for this region
                destClient.close();
            }

            // Print final results
            System.out.println("\n=== Verification Results ===");
            System.out.println("Total records verified: " + totalVerified.get());
            System.out.println("Total mismatches found: " + mismatches.get());
            System.out.println("Verification accuracy: " + 
                String.format("%.2f%%", (totalVerified.get() - mismatches.get()) * 100.0 / totalVerified.get()));

            // Close source connection
            sourceClient.close();

        } catch (Exception e) {
            System.err.println("Error during verification: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static boolean equals(Object obj1, Object obj2) {
        if (obj1 == obj2) {
            return true;
        }
        if (obj1 == null || obj2 == null) {
            return false;
        }
        return obj1.toString().equals(obj2.toString());
    }
} 