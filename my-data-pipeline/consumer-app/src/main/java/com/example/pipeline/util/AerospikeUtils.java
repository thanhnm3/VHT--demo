// package com.example.pipeline.util;

// import com.aerospike.client.AerospikeClient;
// import com.aerospike.client.Bin;
// import com.aerospike.client.Key;
// import com.aerospike.client.policy.WritePolicy;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.core.type.TypeReference;

// import java.util.Base64;
// import java.util.Map;
// import java.nio.charset.StandardCharsets;

// public class AerospikeUtils {
//     private static final ObjectMapper objectMapper = new ObjectMapper();
//     private static final int MAX_RETRIES = 3;

//     public static boolean validateConnection(AerospikeClient client) {
//         try {
//             return client != null && client.isConnected();
//         } catch (Exception e) {
//             return false;
//         }
//     }

//     public static boolean isRetryableError(Exception e) {
//         return e instanceof com.aerospike.client.AerospikeException.Timeout ||
//                e instanceof com.aerospike.client.AerospikeException.Connection;
//     }

//     public static void handleRetry(int currentRetry, Exception e) {
//         long backoffTime = calculateBackoffTime(currentRetry);
//         System.err.printf("Waiting %d ms before retry attempt %d%n", backoffTime, currentRetry);
//         try {
//             Thread.sleep(backoffTime);
//         } catch (InterruptedException ie) {
//             Thread.currentThread().interrupt();
//             throw new RuntimeException("Interrupted during retry", ie);
//         }
//     }

//     public static long calculateBackoffTime(int retryCount) {
//         long baseDelay = 1000;
//         long maxDelay = 10000;
//         long delay = Math.min(baseDelay * (1L << retryCount), maxDelay);
//         return delay + (long)(Math.random() * 1000);
//     }

//     public static void processRecord(byte[] recordKey, byte[] recordValue, 
//                                    AerospikeClient destinationClient,
//                                    WritePolicy writePolicy,
//                                    String destinationNamespace,
//                                    String setName) {
//         int currentRetry = 0;
//         boolean success = false;
//         String recordKeyStr = new String(recordKey, StandardCharsets.UTF_8);

//         while (!success && currentRetry < MAX_RETRIES) {
//             try {
//                 if (!validateConnection(destinationClient)) {
//                     System.err.printf("Aerospike connection error for record %s: Connection is not valid%n", recordKeyStr);
//                     throw new RuntimeException("Aerospike connection is not valid");
//                 }

//                 if (recordKey == null || recordValue == null) {
//                     System.err.printf("Invalid record %s: key or value is null%n", recordKeyStr);
//                     throw new IllegalArgumentException("Invalid record: key or value is null");
//                 }

//                 Key destinationKey = new Key(destinationNamespace, setName, recordKey);

//                 String jsonString = new String(recordValue, StandardCharsets.UTF_8);
//                 Map<String, Object> data = objectMapper.readValue(jsonString, 
//                     new TypeReference<Map<String, Object>>() {});

//                 if (!data.containsKey("personData") || !data.containsKey("lastUpdate")) {
//                     System.err.printf("Invalid data format for record %s: missing required fields%n", recordKeyStr);
//                     throw new IllegalArgumentException("Invalid data format: missing required fields");
//                 }

//                 String personDataBase64 = (String) data.get("personData");
//                 byte[] personData = Base64.getDecoder().decode(personDataBase64);
//                 long lastUpdate = ((Number) data.get("lastUpdate")).longValue();

//                 Bin personBin = new Bin("personData", personData);
//                 Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
//                 Bin keyBin = new Bin("PK", recordKey);

//                 try {
//                     destinationClient.put(writePolicy, destinationKey, keyBin, personBin, lastUpdateBin);
//                     success = true;
//                 } catch (Exception e) {
//                     if (isRetryableError(e)) {
//                         System.err.printf("Retryable error for record %s: %s%n", recordKeyStr, e.getMessage());
//                         throw e;
//                     } else {
//                         System.err.printf("Non-retryable error for record %s: %s%n", recordKeyStr, e.getMessage());
//                         throw new RuntimeException("Non-retryable error during write: " + e.getMessage(), e);
//                     }
//                 }
//             } catch (Exception e) {
//                 currentRetry++;
//                 if (currentRetry < MAX_RETRIES) {
//                     System.err.printf("Retry attempt %d for record %s: %s%n", 
//                         currentRetry, recordKeyStr, e.getMessage());
//                     handleRetry(currentRetry, e);
//                 } else {
//                     System.err.printf("Failed to process record %s after %d attempts: %s%n", 
//                         recordKeyStr, MAX_RETRIES, e.getMessage());
//                     throw new RuntimeException("Failed to process record after " + MAX_RETRIES + " attempts: " + e.getMessage(), e);
//                 }
//             }
//         }
//     }
// } 