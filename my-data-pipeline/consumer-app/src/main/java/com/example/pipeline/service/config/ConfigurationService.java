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
    private int workerPoolSize = 4;  // Default value
    private int maxMessagesPerSecond = 1000;  // Default value
    
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
        
        // Initialize topic mappings for each region
        for (String region : baseConfig.getRegions()) {
            String topicName = TopicGenerator.generateATopicName(
                TopicGenerator.TopicNameGenerator.generateTopicName("producer1", region));
            regionToTopicMap.put(region, topicName);
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
    
    public String getConsumerGroup(String consumerName, String region) {
        String baseGroup = consumerName + "_" + region + "-group";
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