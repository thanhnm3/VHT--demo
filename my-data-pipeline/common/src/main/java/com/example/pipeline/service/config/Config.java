package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;

public class Config {
    private List<Producer> producers;
    private List<Consumer> consumers;
    private Map<String, List<String>> prefix_mapping;
    private KafkaConfig kafka;
    private PerformanceConfig performance;

    public static class Producer {
        private String name;
        private String host;
        private int port;
        private String namespace;
        private String set;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getNamespace() { return namespace; }
        public void setNamespace(String namespace) { this.namespace = namespace; }
        public String getSet() { return set; }
        public void setSet(String set) { this.set = set; }
    }

    public static class Consumer {
        private String name;
        private String host;
        private int port;
        private String namespace;
        private String set;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getNamespace() { return namespace; }
        public void setNamespace(String namespace) { this.namespace = namespace; }
        public String getSet() { return set; }
        public void setSet(String set) { this.set = set; }
    }

    public static class KafkaConfig {
        private Brokers brokers;

        public static class Brokers {
            private String source;
            private String target;

            public String getSource() { return source; }
            public void setSource(String source) { this.source = source; }
            public String getTarget() { return target; }
            public void setTarget(String target) { this.target = target; }
        }

        public Brokers getBrokers() { return brokers; }
        public void setBrokers(Brokers brokers) { this.brokers = brokers; }
    }

    public static class PerformanceConfig {
        private int max_messages_per_second;
        private int max_retries;

        public int getMax_messages_per_second() { return max_messages_per_second; }
        public void setMax_messages_per_second(int max_messages_per_second) { 
            this.max_messages_per_second = max_messages_per_second; 
        }
        public int getMax_retries() { return max_retries; }
        public void setMax_retries(int max_retries) { this.max_retries = max_retries; }
    }

    // Getters và Setters
    public List<Producer> getProducers() { return producers; }
    public void setProducers(List<Producer> producers) { this.producers = producers; }
    public List<Consumer> getConsumers() { return consumers; }
    public void setConsumers(List<Consumer> consumers) { this.consumers = consumers; }
    public Map<String, List<String>> getPrefix_mapping() { return prefix_mapping; }
    public void setPrefix_mapping(Map<String, List<String>> prefix_mapping) { 
        this.prefix_mapping = prefix_mapping; 
    }
    public KafkaConfig getKafka() { return kafka; }
    public void setKafka(KafkaConfig kafka) { this.kafka = kafka; }
    public PerformanceConfig getPerformance() { return performance; }
    public void setPerformance(PerformanceConfig performance) { this.performance = performance; }
}