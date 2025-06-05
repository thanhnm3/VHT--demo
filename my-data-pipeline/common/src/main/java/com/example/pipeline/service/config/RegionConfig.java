package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RegionConfig {
    private List<String> unchanged;
    private Map<String, List<String>> changed;

    public List<String> getUnchanged() {
        return unchanged;
    }

    public void setUnchanged(List<String> unchanged) {
        this.unchanged = unchanged;
    }

    public Map<String, List<String>> getChanged() {
        return changed;
    }

    public void setChanged(Map<String, List<String>> changed) {
        this.changed = changed;
    }

    public int getTotalUnchangedCount() {
        return unchanged != null ? unchanged.size() : 0;
    }

    public int getTotalChangedCount() {
        return changed != null ? changed.size() : 0;
    }

    public int getTotalMergedCount() {
        return changed != null ? changed.values().stream()
                .mapToInt(List::size)
                .sum() : 0;
    }

    public int getTotalProvinces() {
        return getTotalUnchangedCount() + getTotalChangedCount() + getTotalMergedCount();
    }

    public static class ProvinceInfo {
        private final String region;
        private final String correspondingProvince;

        public ProvinceInfo(String region, String correspondingProvince) {
            this.region = region;
            this.correspondingProvince = correspondingProvince;
        }

        public String getRegion() {
            return region;
        }

        public String getCorrespondingProvince() {
            return correspondingProvince;
        }
    }

    public ProvinceInfo findProvinceInfo(String provinceName, Map<String, RegionConfig> regionGroups) {
        // Check in unchanged provinces
        for (Map.Entry<String, RegionConfig> entry : regionGroups.entrySet()) {
            String region = entry.getKey();
            RegionConfig config = entry.getValue();
            
            // Check in unchanged list
            if (config.getUnchanged() != null && config.getUnchanged().contains(provinceName)) {
                return new ProvinceInfo(region, provinceName);
            }
            
            // Check in changed map
            if (config.getChanged() != null) {
                // Check if province is a key in changed map
                if (config.getChanged().containsKey(provinceName)) {
                    return new ProvinceInfo(region, provinceName);
                }
                
                // Check if province is in any of the value lists
                for (Map.Entry<String, List<String>> changedEntry : config.getChanged().entrySet()) {
                    if (changedEntry.getValue().contains(provinceName)) {
                        return new ProvinceInfo(region, changedEntry.getKey());
                    }
                }
            }
        }
        
        return null; // Province not found
    }
} 