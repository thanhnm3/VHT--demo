package com.example.pipeline.service.config;

import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.TopicGenerator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigurationService {
    private static ConfigurationService instance;
    private final Map<String, Config.Consumer> consumerConfigs;
    private final Map<String, List<String>> prefixMappings;
    private final Map<String, String> prefixToTopicMap;
    private Config baseConfig;
    
    // Consumer-specific configurations
    private int workerPoolSize = 4;  // Default value
    private int maxMessagesPerSecond = 1000;  // Default value
    
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
        for (Config.Consumer consumer : baseConfig.getConsumers()) {
            consumerConfigs.put(consumer.getName(), consumer);
        }
        
        // Initialize prefix mappings
        prefixMappings.putAll(baseConfig.getPrefix_mapping());
        
        // Initialize topic mappings
        prefixToTopicMap.putAll(TopicGenerator.generateTopics());
    }
    
    public List<Config.Consumer> getConsumers() {
        return baseConfig.getConsumers();
    }
    
    public Map<String, List<String>> getPrefixMappings() {
        return new HashMap<>(prefixMappings);
    }
    
    public Config.Consumer getConsumerConfig(String consumerName) {
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
    
    // Consumer-specific configuration getters and setters
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