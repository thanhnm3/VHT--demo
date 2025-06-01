package com.example.pipeline.service.config;

import java.util.List;
import java.util.Map;

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
} 