package com.example.pipeline.service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;

public class CdcService {
    private final AerospikeClient aerospikeClient;
    private final WritePolicy writePolicy;
    private final String namespace;
    private final String setName;
    private final ObjectMapper objectMapper;
    private static final Logger logger = LoggerFactory.getLogger(CdcService.class);

    public CdcService(AerospikeClient aerospikeClient, WritePolicy writePolicy, String namespace, String setName) {
        this.aerospikeClient = aerospikeClient;
        this.writePolicy = writePolicy;
        this.namespace = namespace;
        this.setName = setName;
        this.objectMapper = new ObjectMapper();
    }

    public void processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            // Check if key is null
            if (record.key() == null) {
                logger.error("Received null key, skipping record");
                return;
            }

            // Create Aerospike key using record key
            Key aerospikeKey = new Key(namespace, setName, record.key());

            // Parse JSON from Kafka value
            Map<String, Object> data = null;
            if (record.value() != null) {
                String jsonString = new String(record.value(), StandardCharsets.UTF_8);
                try {
                    data = objectMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
                } catch (JsonProcessingException e) {
                    logger.error("Error parsing JSON: {}", e.getMessage());
                    throw new RuntimeException("Failed to parse JSON message", e);
                }
            }

            if (data == null) {
                logger.error("No data to process");
                return;
            }

            // Create bins list for Aerospike
            List<Bin> bins = new ArrayList<>();

            // Add basic fields
            if (data.containsKey("user_id")) {
                bins.add(new Bin("user_id", String.valueOf(data.get("user_id"))));
            }
            if (data.containsKey("phone")) {
                bins.add(new Bin("phone", String.valueOf(data.get("phone"))));
            }
            if (data.containsKey("service_type")) {
                bins.add(new Bin("service_type", String.valueOf(data.get("service_type"))));
            }
            if (data.containsKey("province")) {
                bins.add(new Bin("province", String.valueOf(data.get("province"))));
            }
            if (data.containsKey("region")) {
                bins.add(new Bin("region", String.valueOf(data.get("region"))));
            }

            // Handle last_updated
            Object lastUpdateObj = data.get("last_updated");
            long lastUpdate;
            if (lastUpdateObj != null) {
                try {
                    lastUpdate = ((Number) lastUpdateObj).longValue();
                } catch (Exception e) {
                    logger.error("Error parsing last_updated: {}", e.getMessage());
                    lastUpdate = System.currentTimeMillis();
                }
            } else {
                lastUpdate = System.currentTimeMillis();
            }
            bins.add(new Bin("last_updated", lastUpdate));

            // Handle notes
            if (data.containsKey("notes")) {
                Object notesObj = data.get("notes");
                if (notesObj != null) {
                    bins.add(new Bin("notes", String.valueOf(notesObj)));
                }
            }

            // Write to Aerospike with all bins
            aerospikeClient.put(writePolicy, aerospikeKey, bins.toArray(new Bin[0]));

        } catch (Exception e) {
            logger.error("Error processing record: {}", e.getMessage(), e);
            throw e;
        }
    }
} 