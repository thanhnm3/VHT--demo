package com.example.pipeline.test;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ReadSubscriber {
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        try {
            // Load configuration
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Khong the load cau hinh");
            }

            // Get Producer config
            String producerHost = config.getProducers().get(0).getHost();
            int producerPort = config.getProducers().get(0).getPort();
            String producerNamespace = config.getProducers().get(0).getNamespace();
            String producerSetName = config.getProducers().get(0).getSet();

            System.out.println("=== Bat dau doc du lieu ===");
            System.out.println("Producer Host: " + producerHost);
            System.out.println("Producer Port: " + producerPort);
            System.out.println("Producer Namespace: " + producerNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("===============================");

            // Connect to Aerospike
            AerospikeClient client = new AerospikeClient(producerHost, producerPort);
            System.out.println("Ket noi den Aerospike thanh cong!");

            // Scan policy
            ScanPolicy scanPolicy = new ScanPolicy();
            scanPolicy.sendKey = true;

            // Scan all records
            System.out.println("\n=== Bat dau quet du lieu ===");
            final int[] count = {0};
            client.scanAll(scanPolicy, producerNamespace, producerSetName, (key, record) -> {
                if (count[0] < 3) {
                    System.out.println("\n=== Ban ghi " + (count[0] + 1) + " ===");
                    System.out.println("Key: " + key.userKey);
                    
                    // Get subscriber data with type-safe cast
                    Object value = record.getValue("sub");
                    if (!(value instanceof Map)) {
                        throw new IllegalStateException("Expected Map but got: " + value.getClass());
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> subscriberData = (Map<String, Object>) value;
                    
                    // Print all fields
                    System.out.println("\n=== Thong tin subscriber ===");
                    System.out.println("Region: " + subscriberData.get("r"));
                    System.out.println("Province: " + subscriberData.get("p"));
                    
                    // Format last update timestamp
                    Object lastUpdate = subscriberData.get("lu");
                    if (lastUpdate instanceof Number) {
                        long timestamp = ((Number) lastUpdate).longValue();
                        System.out.println("Last Updated: " + dateFormat.format(new Date(timestamp)));
                    } else {
                        System.out.println("Last Updated: " + lastUpdate);
                    }
                    
                    System.out.println("MSISDN: " + subscriberData.get("m"));
                    System.out.println("Subscriber ID: " + subscriberData.get("si"));
                    System.out.println("Customer ID: " + subscriberData.get("ci"));
                    System.out.println("Customer Type: " + subscriberData.get("ct"));
                    System.out.println("Registration Type: " + subscriberData.get("rt"));
                    System.out.println("State Set: " + subscriberData.get("ss"));
                    System.out.println("Subscriber Type: " + subscriberData.get("st"));
                    
                    // Print full data in JSON format
                    System.out.println("\n=== Toan bo du lieu (JSON) ===");
                    System.out.println(gson.toJson(subscriberData));
                    
                    count[0]++;
                }
            });

            if (count[0] == 0) {
                System.out.println("Khong tim thay ban ghi nao trong database");
            }

            client.close();
        } catch (Exception e) {
            System.err.println("Loi: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 