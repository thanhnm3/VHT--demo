package com.example.pipeline.service.config;

import java.util.List;

public class ProducerConfig {
    private List<Producer> producers;

    public static class Producer {
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

    // Getters và Setters cho producers
    public List<Producer> getProducers() { return producers; }
    public void setProducers(List<Producer> producers) { this.producers = producers; }
}
