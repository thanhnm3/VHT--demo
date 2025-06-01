package com.example.pipeline.full;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.config.RegionConfig;

import java.util.List;
import java.util.Map;

public class Region {
    public static void main(String[] args) {
        try {
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }
            printRegionInfo(config);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printRegionInfo(Config config) {
        Map<String, RegionConfig> regionGroups = config.getRegion_groups();
        
        System.out.println("=== THONG TIN PHAN VUNG ===");
        
        printRegion("MIEN BAC", regionGroups.get("north"));
        printRegion("MIEN TRUNG", regionGroups.get("central"));
        printRegion("MIEN NAM", regionGroups.get("south"));
    }

    private static void printRegion(String regionName, RegionConfig region) {
        System.out.println("\n" + regionName + ":");
        
        if (region == null) {
            System.out.println("Khong co thong tin cho vung nay");
            return;
        }

        // In danh sách tỉnh không thay đổi
        System.out.println("\nCac tinh khong thay doi:");
        List<String> unchanged = region.getUnchanged();
        if (unchanged != null) {
            for (int i = 0; i < unchanged.size(); i++) {
                System.out.printf("%d. %s%n", i + 1, unchanged.get(i));
            }
        }

        // In danh sách tỉnh đã thay đổi
        System.out.println("\nCac tinh da thay doi:");
        Map<String, List<String>> changed = region.getChanged();
        if (changed != null) {
            for (Map.Entry<String, List<String>> entry : changed.entrySet()) {
                String mainProvince = entry.getKey();
                List<String> mergedProvinces = entry.getValue();
                for (String mergedProvince : mergedProvinces) {
                    System.out.printf("%s se sap nhap vao %s%n", mergedProvince, mainProvince);
                }
            }
        }

        // In thống kê
        System.out.printf("%nTong so tinh: %d%n", region.getTotalProvinces());
        System.out.printf("So tinh khong thay doi: %d%n", region.getTotalUnchangedCount());
        System.out.printf("So tinh chu dau: %d%n", region.getTotalChangedCount());
        System.out.printf("So tinh duoc sap nhap: %d%n", region.getTotalMergedCount());
    }
}
