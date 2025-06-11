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
    private final Map<String, List<String>> regionMappings;
    private final Map<String, String> regionToTopicMap;
    private Config baseConfig;
    
    // Consumer-specific configurations
    private int workerPoolSize;
    private int maxMessagesPerSecond;
    
    private ConfigurationService() {
        this.consumerConfigs = new ConcurrentHashMap<>();
        this.regionMappings = new ConcurrentHashMap<>();
        this.regionToTopicMap = new ConcurrentHashMap<>();
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
        
        // Initialize region mappings
        regionMappings.putAll(baseConfig.getRegion_mapping());
        
        // Initialize topic mappings
        regionToTopicMap.putAll(TopicGenerator.generateTopics());

        // Initialize performance configurations
        Config.PerformanceConfig perfConfig = baseConfig.getPerformance();
        if (perfConfig != null) {
            this.workerPoolSize = perfConfig.getWorker_pool().getConsumer();
            this.maxMessagesPerSecond = perfConfig.getMax_messages_per_second();
        } else {
            // Default values if performance config is not available
            this.workerPoolSize = 4;
            this.maxMessagesPerSecond = 1000;
        }
    }
    
    public List<Config.Consumer> getConsumers() {
        return baseConfig.getConsumers();
    }
    
    public Map<String, List<String>> getRegionMappings() {
        return new HashMap<>(regionMappings);
    }
    
    public Config.Consumer getConsumerConfig(String consumerName) {
        return consumerConfigs.get(consumerName);
    }
    
    public List<String> getConsumerNamesForRegion(String region) {
        return regionMappings.get(region);
    }
    
    public String getTopicForRegion(String region) {
        return regionToTopicMap.get(region);
    }
    
    public Map<String, String> getAllRegionToTopicMappings() {
        return new HashMap<>(regionToTopicMap);
    }
    
    public String getConsumerGroup(String consumerName) {
        String baseGroup = consumerName + "-group";
        if (consumerName.endsWith("-cdc")) {
            return baseGroup + "-cdc";
        }
        return baseGroup;
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