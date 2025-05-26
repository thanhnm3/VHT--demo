package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Base64;
import java.util.Map;
import java.nio.charset.StandardCharsets;

public class CdcService {
    private final AerospikeClient aerospikeClient;
    private final WritePolicy writePolicy;
    private final String namespace;
    private final String setName;
    private final ObjectMapper objectMapper;

    public CdcService(AerospikeClient aerospikeClient, WritePolicy writePolicy, String namespace, String setName) {
        this.aerospikeClient = aerospikeClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.objectMapper = new ObjectMapper();
    }

    public void processRecord(ConsumerRecord<byte[], byte[]> record) {
        final int MAX_RETRIES = 3;
        int retryCount = 0;

        while (retryCount <= MAX_RETRIES) {
            try {
                byte[] keyBytes = record.key();
                byte[] value = record.value();

                if (keyBytes == null) {
                    System.err.println("Nhan duoc key null, bo qua record.");
                    return;
                }

                // Tao key tu Kafka key
                byte[] userId = record.key();
                Key aerospikeKey = new Key(namespace, setName, userId);

                // Giai ma JSON tu Kafka value
                Map<String, Object> data = null;
                if (value != null) {
                    String jsonString = new String(value, StandardCharsets.UTF_8);
                    data = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
                }

                // Xu ly personData
                String personDataBase64 = data != null ? (String) data.get("personData") : null;
                byte[] personData = null;
                if (personDataBase64 != null) {
                    try {
                        personData = Base64.getDecoder().decode(personDataBase64);
                    } catch (IllegalArgumentException e) {
                        personData = null;
                    }
                }

                // Xu ly lastUpdate
                long lastUpdate = 0;
                if (data != null) {
                    Object lastUpdateObj = data.get("lastUpdate");
                    if (lastUpdateObj != null) {
                        try {
                            lastUpdate = ((Number) lastUpdateObj).longValue();
                        } catch (Exception e) {
                            // Sử dụng giá trị mặc định 0
                        }
                    }
                }

                // Tao va luu cac bin
                Bin personBin = new Bin("personData", personData);
                Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);

                aerospikeClient.put(writePolicy, aerospikeKey, personBin, lastUpdateBin);
                return;

            } catch (Exception e) {
                retryCount++;
                System.err.println("Loi xu ly record (lan thu " + retryCount + "): " + e.getMessage());

                if (retryCount > MAX_RETRIES) {
                    System.err.println("Da vuot qua so lan retry toi da. Bo qua record.");
                    e.printStackTrace();
                    return;
                }

                try {
                    Thread.sleep(100 * retryCount);
                } catch (InterruptedException ie) {
                    System.err.println("Bi ngat khi cho retry: " + ie.getMessage());
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
} 