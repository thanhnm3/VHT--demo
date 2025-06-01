package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
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
        try {
            byte[] keyBytes = record.key();
            if (keyBytes == null) {
                System.err.println("Nhan duoc key null, bo qua record.");
                return;
            }

            // Tao key tu Kafka key
            Key aerospikeKey = new Key(namespace, setName, keyBytes);

            // Giai ma JSON tu Kafka value
            Map<String, Object> data = null;
            if (record.value() != null) {
                String jsonString = new String(record.value(), StandardCharsets.UTF_8);
                try {
                    data = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
                } catch (JsonProcessingException e) {
                    System.err.println("Loi giai ma JSON: " + e.getMessage());
                    data = null;
                }
            }

            // Xu ly personData
            String personDataBase64 = data != null ? (String) data.get("personData") : null;
            byte[] personData = null;
            if (personDataBase64 != null) {
                try {
                    personData = Base64.getDecoder().decode(personDataBase64);
                } catch (IllegalArgumentException e) {
                    System.err.println("Loi giai ma personData: " + e.getMessage());
                    personData = null;
                }
            }

            // Xu ly lastUpdate
            long lastUpdate = System.currentTimeMillis(); // Default to current time
            if (data != null) {
                Object lastUpdateObj = data.get("lastUpdate");
                if (lastUpdateObj != null) {
                    try {
                        lastUpdate = ((Number) lastUpdateObj).longValue();
                    } catch (Exception e) {
                        System.err.println("Loi xu ly lastUpdate: " + e.getMessage());
                        // Giữ nguyên giá trị mặc định là thời gian hiện tại
                    }
                }
            }

            // Tạo bins và ghi vào Aerospike
            Bin personDataBin = new Bin("personData", personData);
            Bin lastUpdateBin = new Bin("lastUpdate", lastUpdate);
            aerospikeClient.put(writePolicy, aerospikeKey, personDataBin, lastUpdateBin);

        } catch (Exception e) {
            System.err.println("Loi xu ly record: " + e.getMessage());
            throw e;
        }
    }
} 