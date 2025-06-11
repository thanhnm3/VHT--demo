package com.example.pipeline.service.config;

import java.util.List;
import com.example.pipeline.service.ConfigLoader;

public class ConfigProducerService {
    private static ConfigProducerService instance;
    private Config config;
    
    // Producer-specific configurations
    private int workerPoolSize;
    private int maxMessagesPerSecond;
    private double maxRate;
    private double minRate;
    private double initialRate;
    private int lagThreshold;
    private int monitoringIntervalSeconds;
    private int maxRetries;
    private int rateAdjustmentSteps;
    private double maxRateChangePercent;
    private String sourceNamespace;
    private String setName;
    private String aerospikeHost;
    private int aerospikePort;
    private String kafkaBroker;
    private String consumerGroup;
    
    private ConfigProducerService() {
        loadConfiguration();
    }
    
    public static synchronized ConfigProducerService getInstance() {
        if (instance == null) {
            instance = new ConfigProducerService();
        }
        return instance;
    }
    
    private void loadConfiguration() {
        try {
            config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            List<Config.Producer> producers = config.getProducers();
            if (producers != null && !producers.isEmpty()) {
                Config.Producer producer = producers.get(0);
                this.sourceNamespace = producer.getNamespace();
                this.setName = producer.getSet();
                this.aerospikeHost = producer.getHost();
                this.aerospikePort = producer.getPort();
            }
            
            // Load Kafka configuration
            Config.KafkaConfig kafkaConfig = config.getKafka();
            if (kafkaConfig != null) {
                this.kafkaBroker = kafkaConfig.getBroker();
            }
            
            // Load Performance configuration
            Config.PerformanceConfig perfConfig = config.getPerformance();
            if (perfConfig != null) {
                this.maxMessagesPerSecond = perfConfig.getMax_messages_per_second();
                this.maxRetries = perfConfig.getMax_retries();
                
                // Load worker pool configuration
                if (perfConfig.getWorker_pool() != null) {
                    this.workerPoolSize = perfConfig.getWorker_pool().getProducer();
                }
                
                // Load rate control configuration
                if (perfConfig.getRate_control() != null) {
                    Config.RateControlConfig rateConfig = perfConfig.getRate_control();
                    this.initialRate = rateConfig.getInitial_rate();
                    this.maxRate = rateConfig.getMax_rate();
                    this.minRate = rateConfig.getMin_rate();
                    this.lagThreshold = rateConfig.getLag_threshold();
                    this.monitoringIntervalSeconds = rateConfig.getMonitoring_interval_seconds();
                    this.rateAdjustmentSteps = rateConfig.getRate_adjustment_steps();
                    this.maxRateChangePercent = rateConfig.getMax_rate_change_percent();
                }
            }

            // Verify region mapping is loaded
            if (config.getRegion_mapping() == null) {
                throw new IllegalStateException("Region mapping is not configured");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error loading configuration: " + e.getMessage(), e);
        }
    }
    
    // Region-based configuration methods
    public List<String> getConsumersForRegion(String region) {
        return config.getConsumersForRegion(region);
    }
    
    public String getRegionForConsumer(String consumerName) {
        return config.getRegionForConsumer(consumerName);
    }
    
    public List<String> getConsumersForProvince(String province) {
        return config.getConsumersForProvince(province);
    }
    
    public boolean isProvinceInRegion(String province, String region) {
        return config.getRegion_groups().isProvinceInRegion(province, region);
    }
    
    public String getRegionOfProvince(String province) {
        return config.getRegion_groups().getRegionOfProvince(province);
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
    
    public double getInitialRate() {
        return initialRate;
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
    
    public int getRateAdjustmentSteps() {
        return rateAdjustmentSteps;
    }
    
    public double getMaxRateChangePercent() {
        return maxRateChangePercent;
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