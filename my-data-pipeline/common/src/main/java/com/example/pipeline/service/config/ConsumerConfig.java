package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;

public class ConsumerConfig {
    private List<Consumer> consumers;
    private Map<String, List<String>> region_mapping;
    private RegionGroups region_groups;

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

    public static class RegionGroups {
        private List<String> north;
        private List<String> central;
        private List<String> south;

        public List<String> getNorth() { return north; }
        public void setNorth(List<String> north) { this.north = north; }
        public List<String> getCentral() { return central; }
        public void setCentral(List<String> central) { this.central = central; }
        public List<String> getSouth() { return south; }
        public void setSouth(List<String> south) { this.south = south; }

        public List<String> getProvincesByRegion(String region) {
            return switch (region.toLowerCase()) {
                case "north" -> north;
                case "central" -> central;
                case "south" -> south;
                default -> throw new IllegalArgumentException("Invalid region: " + region);
            };
        }

        public boolean isProvinceInRegion(String province, String region) {
            List<String> provinces = getProvincesByRegion(region);
            return provinces != null && provinces.contains(province);
        }

        public String getRegionOfProvince(String province) {
            if (north != null && north.contains(province)) return "north";
            if (central != null && central.contains(province)) return "central";
            if (south != null && south.contains(province)) return "south";
            return null;
        }
    }

    // Getters và Setters cho consumers
    public List<Consumer> getConsumers() { return consumers; }
    public void setConsumers(List<Consumer> consumers) { this.consumers = consumers; }

    // Getters và Setters cho region_mapping
    public Map<String, List<String>> getRegion_mapping() { return region_mapping; }
    public void setRegion_mapping(Map<String, List<String>> region_mapping) { 
        this.region_mapping = region_mapping; 
    }

    // Getters và Setters cho region_groups
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
}