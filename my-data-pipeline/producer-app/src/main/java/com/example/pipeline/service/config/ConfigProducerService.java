package com.example.pipeline.service.config;

import com.example.pipeline.service.TopicGenerator;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

public class ConfigProducerService {
    private static ConfigProducerService instance;
    private final Map<String, String> prefixToTopicMap;
    private Config config;
    
    // Producer-specific configurations
    private int workerPoolSize = 4;  // Default value
    private int maxMessagesPerSecond = 5000;  // Default value
    private double maxRate = 100000.0;  // Default value
    private double minRate = 1000.0;  // Default value
    private int lagThreshold = 1000;  // Default value
    private int monitoringIntervalSeconds = 10;  // Default value
    private int maxRetries = 5;  // Default value
    private String sourceNamespace;
    private String setName;
    private String aerospikeHost;
    private int aerospikePort;
    private String kafkaBroker;
    private String consumerGroup;
    
    private ConfigProducerService() {
        this.prefixToTopicMap = new ConcurrentHashMap<>();
        loadConfiguration();
    }
    
    public static synchronized ConfigProducerService getInstance() {
        if (instance == null) {
            instance = new ConfigProducerService();
        }
        return instance;
    }
    
    private void loadConfiguration() {
        config = new Config();
        List<Config.Producer> producers = config.getProducers();
        
        if (producers != null && !producers.isEmpty()) {
            Config.Producer producer = producers.get(0); // Lấy producer đầu tiên
            this.sourceNamespace = producer.getNamespace();
            this.setName = producer.getSet();
            this.aerospikeHost = producer.getHost();
            this.aerospikePort = producer.getPort();
        }
        
        // Lấy cấu hình Kafka
        Config.KafkaConfig kafkaConfig = config.getKafka();
        if (kafkaConfig != null) {
            this.kafkaBroker = kafkaConfig.getBroker();
        }
        
        // Lấy cấu hình Performance
        Config.PerformanceConfig perfConfig = config.getPerformance();
        if (perfConfig != null) {
            this.maxMessagesPerSecond = perfConfig.getMax_messages_per_second();
            this.maxRetries = perfConfig.getMax_retries();
        }
        
        // Initialize topic mappings
        prefixToTopicMap.putAll(TopicGenerator.generateTopics());
    }
    
    public Map<String, String> getPrefixToTopicMap() {
        return new HashMap<>(prefixToTopicMap);
    }
    
    public String getTopicForPrefix(String prefix) {
        return prefixToTopicMap.get(prefix);
    }
    
    // Producer-specific configuration getters
    public int getWorkerPoolSize() {
        return workerPoolSize;
    }
    
    public int getMaxMessagesPerSecond() {
        return maxMessagesPerSecond;
    }
    
    public double getMaxRate() {
        return maxRate;
    }
    
    public double getMinRate() {
        return minRate;
    }
    
    public int getLagThreshold() {
        return lagThreshold;
    }
    
    public int getMonitoringIntervalSeconds() {
        return monitoringIntervalSeconds;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public String getSourceNamespace() {
        return sourceNamespace;
    }
    
    public String getSetName() {
        return setName;
    }
    
    public String getAerospikeHost() {
        return aerospikeHost;
    }
    
    public int getAerospikePort() {
        return aerospikePort;
    }
    
    public String getKafkaBroker() {
        return kafkaBroker;
    }
    
    public String getConsumerGroup() {
        return consumerGroup;
    }
    
    public void reloadConfiguration() {
        loadConfiguration();
    }
} 