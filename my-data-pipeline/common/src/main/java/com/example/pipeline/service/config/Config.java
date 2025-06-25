package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;

public class Config {
    private List<Producer> producers;
    private List<Consumer> consumers;
    private Map<String, List<String>> region_mapping;
    private KafkaConfig kafka;
    private PerformanceConfig performance;
    private RegionGroups region_groups;

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
        private String broker;

        public String getBroker() { return broker; }
        public void setBroker(String broker) { this.broker = broker; }
    }

    public static class PerformanceConfig {
        private int max_messages_per_second;
        private int max_retries;
        private WorkerPoolConfig worker_pool;
        private RateControlConfig rate_control;

        public int getMax_messages_per_second() {
            return max_messages_per_second;
        }

        public void setMax_messages_per_second(int max_messages_per_second) {
            this.max_messages_per_second = max_messages_per_second;
        }

        public int getMax_retries() {
            return max_retries;
        }

        public void setMax_retries(int max_retries) {
            this.max_retries = max_retries;
        }

        public WorkerPoolConfig getWorker_pool() {
            return worker_pool;
        }

        public void setWorker_pool(WorkerPoolConfig worker_pool) {
            this.worker_pool = worker_pool;
        }

        public RateControlConfig getRate_control() {
            return rate_control;
        }

        public void setRate_control(RateControlConfig rate_control) {
            this.rate_control = rate_control;
        }
    }

    public static class WorkerPoolConfig {
        private int producer;
        private int consumer;

        public int getProducer() {
            return producer;
        }

        public void setProducer(int producer) {
            this.producer = producer;
        }

        public int getConsumer() {
            return consumer;
        }

        public void setConsumer(int consumer) {
            this.consumer = consumer;
        }
    }

    public static class RateControlConfig {
        private double initial_rate;
        private double max_rate;
        private double min_rate;
        private int lag_threshold;
        private int monitoring_interval_seconds;
        private int rate_adjustment_steps;
        private double max_rate_change_percent;

        public double getInitial_rate() { return initial_rate; }
        public void setInitial_rate(double initial_rate) { this.initial_rate = initial_rate; }
        public double getMax_rate() { return max_rate; }
        public void setMax_rate(double max_rate) { this.max_rate = max_rate; }
        public double getMin_rate() { return min_rate; }
        public void setMin_rate(double min_rate) { this.min_rate = min_rate; }
        public int getLag_threshold() { return lag_threshold; }
        public void setLag_threshold(int lag_threshold) { this.lag_threshold = lag_threshold; }
        public int getMonitoring_interval_seconds() { return monitoring_interval_seconds; }
        public void setMonitoring_interval_seconds(int monitoring_interval_seconds) { 
            this.monitoring_interval_seconds = monitoring_interval_seconds; 
        }
        public int getRate_adjustment_steps() { return rate_adjustment_steps; }
        public void setRate_adjustment_steps(int rate_adjustment_steps) { 
            this.rate_adjustment_steps = rate_adjustment_steps; 
        }
        public double getMax_rate_change_percent() { return max_rate_change_percent; }
        public void setMax_rate_change_percent(double max_rate_change_percent) { 
            this.max_rate_change_percent = max_rate_change_percent; 
        }
    }

    public static class RegionGroups {
        private Region north;
        private Region central;
        private Region south;

        public static class Region {
            private List<String> unchanged;
            private Map<String, List<String>> merged;

            public List<String> getUnchanged() { return unchanged; }
            public void setUnchanged(List<String> unchanged) { this.unchanged = unchanged; }
            public Map<String, List<String>> getMerged() { return merged; }
            public void setMerged(Map<String, List<String>> merged) { this.merged = merged; }
        }

        public Region getNorth() { return north; }
        public void setNorth(Region north) { this.north = north; }
        public Region getCentral() { return central; }
        public void setCentral(Region central) { this.central = central; }
        public Region getSouth() { return south; }
        public void setSouth(Region south) { this.south = south; }

        public Region getRegionByName(String regionName) {
            return switch (regionName.toLowerCase()) {
                case "north" -> north;
                case "central" -> central;
                case "south" -> south;
                default -> null;
            };
        }

        public String getRegionOfProvince(String province) {
            if (north != null && isProvinceInRegion(province, north)) return "north";
            if (central != null && isProvinceInRegion(province, central)) return "central";
            if (south != null && isProvinceInRegion(province, south)) return "south";
            return null;
        }

        private boolean isProvinceInRegion(String province, Region region) {
            // Kiểm tra trong unchanged
            if (region.getUnchanged() != null && region.getUnchanged().contains(province)) {
                return true;
            }
            
            // Kiểm tra trong merged (cả key và value)
            if (region.getMerged() != null) {
                // Kiểm tra nếu province là key (tỉnh sau sát nhập)
                if (region.getMerged().containsKey(province)) {
                    return true;
                }
                // Kiểm tra nếu province là value (tỉnh trước sát nhập)
                for (List<String> mergedProvinces : region.getMerged().values()) {
                    if (mergedProvinces.contains(province)) {
                        return true;
                    }
                }
            }
            
            return false;
        }

        /**
         * Tìm tên tỉnh sau sát nhập dựa trên tên tỉnh đầu vào
         * @param provinceName Tên tỉnh cần tìm
         * @return Tên tỉnh sau sát nhập, hoặc null nếu không tìm thấy
         */
        public String getMergedProvinceName(String provinceName) {
            // Kiểm tra trong tất cả các vùng
            String result = getMergedProvinceNameInRegion(provinceName, north);
            if (result != null) return result;
            
            result = getMergedProvinceNameInRegion(provinceName, central);
            if (result != null) return result;
            
            result = getMergedProvinceNameInRegion(provinceName, south);
            return result;
        }

        private String getMergedProvinceNameInRegion(String provinceName, Region region) {
            if (region == null) return null;
            
            // Kiểm tra trong unchanged - nếu là tỉnh không thay đổi thì trả về chính nó
            if (region.getUnchanged() != null && region.getUnchanged().contains(provinceName)) {
                return provinceName;
            }
            
            // Kiểm tra trong merged
            if (region.getMerged() != null) {
                // Nếu provinceName là key (tỉnh sau sát nhập), trả về chính nó
                if (region.getMerged().containsKey(provinceName)) {
                    return provinceName;
                }
                
                // Nếu provinceName là value (tỉnh trước sát nhập), tìm key tương ứng
                for (Map.Entry<String, List<String>> entry : region.getMerged().entrySet()) {
                    if (entry.getValue().contains(provinceName)) {
                        return entry.getKey();
                    }
                }
            }
            
            return null;
        }

        // Backward compatibility methods
        public List<String> getProvincesByRegion(String region) {
            Region regionObj = getRegionByName(region);
            if (regionObj == null) return null;
            
            // Tạo danh sách tổng hợp từ unchanged và merged
            List<String> allProvinces = new java.util.ArrayList<>();
            
            if (regionObj.getUnchanged() != null) {
                allProvinces.addAll(regionObj.getUnchanged());
            }
            
            if (regionObj.getMerged() != null) {
                // Thêm các tỉnh sau sát nhập (keys)
                allProvinces.addAll(regionObj.getMerged().keySet());
                // Thêm các tỉnh trước sát nhập (values)
                for (List<String> mergedProvinces : regionObj.getMerged().values()) {
                    allProvinces.addAll(mergedProvinces);
                }
            }
            
            return allProvinces;
        }

        public boolean isProvinceInRegion(String province, String region) {
            List<String> provinces = getProvincesByRegion(region);
            return provinces != null && provinces.contains(province);
        }
    }

    // Getters và Setters
    public List<Producer> getProducers() { return producers; }
    public void setProducers(List<Producer> producers) { this.producers = producers; }
    public List<Consumer> getConsumers() { return consumers; }
    public void setConsumers(List<Consumer> consumers) { this.consumers = consumers; }
    public Map<String, List<String>> getRegion_mapping() { return region_mapping; }
    public void setRegion_mapping(Map<String, List<String>> region_mapping) { 
        this.region_mapping = region_mapping; 
    }
    public KafkaConfig getKafka() { return kafka; }
    public void setKafka(KafkaConfig kafka) { this.kafka = kafka; }
    public PerformanceConfig getPerformance() { return performance; }
    public void setPerformance(PerformanceConfig performance) { 
        this.performance = performance; 
    }
    public RegionGroups getRegion_groups() { return region_groups; }
    public void setRegion_groups(RegionGroups region_groups) { this.region_groups = region_groups; }

    // Helper methods for region mapping
    public List<String> getConsumersForRegion(String region) {
        return region_mapping.get(region.toLowerCase());
    }

    public String getRegionForConsumer(String consumerName) {
        for (Map.Entry<String, List<String>> entry : region_mapping.entrySet()) {
            if (entry.getValue().contains(consumerName)) {
                return entry.getKey();
            }
        }
        return null;
    }

    public List<String> getConsumersForProvince(String province) {
        String region = region_groups.getRegionOfProvince(province);
        return region != null ? getConsumersForRegion(region) : null;
    }

    /**
     * Tìm tên tỉnh sau sát nhập dựa trên tên tỉnh đầu vào
     * @param provinceName Tên tỉnh cần tìm
     * @return Tên tỉnh sau sát nhập, hoặc null nếu không tìm thấy
     */
    public String getMergedProvinceName(String provinceName) {
        if (region_groups == null) return null;
        return region_groups.getMergedProvinceName(provinceName);
    }
}