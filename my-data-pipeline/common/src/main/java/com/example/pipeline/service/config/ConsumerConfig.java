package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;

public class ConsumerConfig {
    private List<Consumer> consumers;
    private Map<String, List<String>> region_mapping;

    public static class Consumer {
        private String name;
        private String host;
        private int port;
        private String namespace;
        private String set;

        // Getters và Setters
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

    // Getters và Setters cho consumers
    public List<Consumer> getConsumers() { return consumers; }
    public void setConsumers(List<Consumer> consumers) { this.consumers = consumers; }

    // Getters và Setters cho region_mapping
    public Map<String, List<String>> getRegion_mapping() { return region_mapping; }
    public void setRegion_mapping(Map<String, List<String>> region_mapping) { 
        this.region_mapping = region_mapping; 
    }
}