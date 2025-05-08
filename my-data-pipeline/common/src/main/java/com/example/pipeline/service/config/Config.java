package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;

public class Config {
    private List<ProducerConfig> producers;
    private List<ConsumerConfig> consumers;
    private Map<String, List<String>> prefix_mapping; // Đổi tên thành prefix_mapping

    // Getters và Setters
    public List<ProducerConfig> getProducers() {
        return producers;
    }

    public void setProducers(List<ProducerConfig> producers) {
        this.producers = producers;
    }

    public List<ConsumerConfig> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<ConsumerConfig> consumers) {
        this.consumers = consumers;
    }

    public Map<String, List<String>> getPrefix_mapping() { // Đổi tên getter
        return prefix_mapping;
    }

    public void setPrefix_mapping(Map<String, List<String>> prefix_mapping) { // Đổi tên setter
        this.prefix_mapping = prefix_mapping;
    }
}