package com.example.pipeline.service.config;

import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigurationService {
    private static ConfigurationService instance;
    private final Map<String, ConsumerConfig> consumerConfigs;
    private final Map<String, List<String>> prefixMappings;
    private final Map<String, String> prefixToTopicMap;
    private Config baseConfig;
    
    private ConfigurationService() {
        this.consumerConfigs = new ConcurrentHashMap<>();
        this.prefixMappings = new ConcurrentHashMap<>();
        this.prefixToTopicMap = new ConcurrentHashMap<>();
        this.baseConfig = loadBaseConfig();
        initializeConfigurations();
    }
    
    public static synchronized ConfigurationService getInstance() {
        if (instance == null) {
            instance = new ConfigurationService();
        }
        return instance;
    }
    
    private Config loadBaseConfig() {
        Config config = ConfigLoader.getConfig();
        if (config == null) {
            throw new IllegalStateException("Failed to load base configuration");
        }
        return config;
    }
    
    private void initializeConfigurations() {
        // Initialize consumer configurations
        for (ConsumerConfig consumerConfig : baseConfig.getConsumers()) {
            consumerConfigs.put(consumerConfig.getName(), consumerConfig);
        }
        
        // Initialize prefix mappings
        prefixMappings.putAll(baseConfig.getPrefix_mapping());
        
        // Initialize topic mappings
        prefixToTopicMap.putAll(TopicGenerator.generateTopics());
    }
    
    public List<ConsumerConfig> getConsumers() {
        return baseConfig.getConsumers();
    }
    
    public Map<String, List<String>> getPrefixMappings() {
        return new HashMap<>(prefixMappings);
    }
    
    public ConsumerConfig getConsumerConfig(String consumerName) {
        return consumerConfigs.get(consumerName);
    }
    
    public List<String> getConsumerNamesForPrefix(String prefix) {
        return prefixMappings.get(prefix);
    }
    
    public String getTopicForPrefix(String prefix) {
        return prefixToTopicMap.get(prefix);
    }
    
    public Map<String, String> getAllPrefixToTopicMappings() {
        return new HashMap<>(prefixToTopicMap);
    }
    
    public String getConsumerGroup(String consumerName) {
        return consumerName + "-group";
    }
    
    public int getWorkerPoolSize() {
        return baseConfig.getWorkerPoolSize();
    }
    
    public int getMaxMessagesPerSecond() {
        return baseConfig.getMaxMessagesPerSecond();
    }
    
    public String getSourceHost() {
        return baseConfig.getSourceHost();
    }
    
    public int getSourcePort() {
        return baseConfig.getSourcePort();
    }
    
    public String getSourceNamespace() {
        return baseConfig.getSourceNamespace();
    }
    
    public String getDestinationHost() {
        return baseConfig.getDestinationHost();
    }
    
    public int getDestinationPort() {
        return baseConfig.getDestinationPort();
    }
    
    public String getKafkaBroker() {
        return baseConfig.getKafkaBroker();
    }
    
    public void reloadConfiguration() {
        Config newConfig = ConfigLoader.getConfig();
        if (newConfig != null) {
            synchronized (this) {
                this.baseConfig = newConfig;
                initializeConfigurations();
            }
        }
    }
} 