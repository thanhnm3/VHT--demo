package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;

public class Config {
    private List<ConsumerConfig> consumers;
    private List<ProducerConfig> producers;
    private Map<String, List<String>> prefix_mapping;
    private int workerPoolSize;
    private int maxMessagesPerSecond;
    private String sourceHost;
    private int sourcePort;
    private String sourceNamespace;
    private String destinationHost;
    private int destinationPort;
    private String kafkaBroker;

    public List<ConsumerConfig> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<ConsumerConfig> consumers) {
        this.consumers = consumers;
    }

    public List<ProducerConfig> getProducers() {
        return producers;
    }

    public void setProducers(List<ProducerConfig> producers) {
        this.producers = producers;
    }

    public Map<String, List<String>> getPrefix_mapping() {
        return prefix_mapping;
    }

    public void setPrefix_mapping(Map<String, List<String>> prefix_mapping) {
        this.prefix_mapping = prefix_mapping;
    }

    public int getWorkerPoolSize() {
        return workerPoolSize;
    }

    public void setWorkerPoolSize(int workerPoolSize) {
        this.workerPoolSize = workerPoolSize;
    }

    public int getMaxMessagesPerSecond() {
        return maxMessagesPerSecond;
    }

    public void setMaxMessagesPerSecond(int maxMessagesPerSecond) {
        this.maxMessagesPerSecond = maxMessagesPerSecond;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public int getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }

    public String getSourceNamespace() {
        return sourceNamespace;
    }

    public void setSourceNamespace(String sourceNamespace) {
        this.sourceNamespace = sourceNamespace;
    }

    public String getDestinationHost() {
        return destinationHost;
    }

    public void setDestinationHost(String destinationHost) {
        this.destinationHost = destinationHost;
    }

    public int getDestinationPort() {
        return destinationPort;
    }

    public void setDestinationPort(int destinationPort) {
        this.destinationPort = destinationPort;
    }

    public String getKafkaBroker() {
        return kafkaBroker;
    }

    public void setKafkaBroker(String kafkaBroker) {
        this.kafkaBroker = kafkaBroker;
    }
} 