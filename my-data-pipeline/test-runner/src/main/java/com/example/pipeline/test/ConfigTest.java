package com.example.pipeline.test;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.Arrays;
import java.util.List;

public class ConfigTest {
    
    public static void main(String[] args) {
        // Load configuration
        Config config = ConfigLoader.getConfig();
        if (config == null) {
            System.err.println("Cannot load configuration");
            return;
        }

        System.out.println("=== TEST HÀM getMergedProvinceName ===");
        System.out.println();

        // Danh sách các tỉnh để test
        List<String> testProvinces = Arrays.asList(
            // Các tỉnh miền Bắc
            "Ha Noi", "Lai Chau", "Dien Bien", "Son La", "Lang Son", "Quang Ninh", "Cao Bang",
            "Ha Giang", "Yen Bai", "Bac Kan", "Vinh Phuc", "Hoa Binh", "Bac Giang", 
            "Thai Binh", "Hai Duong", "Ha Nam", "Nam Dinh",
            "Tuyen Quang", "Lao Cai", "Thai Nguyen", "Phu Tho", "Bac Ninh", "Hung Yen", 
            "Hai Phong", "Ninh Binh",
            
            // Các tỉnh miền Trung
            "Hue", "Thanh Hoa", "Nghe An", "Ha Tinh",
            "Quang Binh", "Quang Nam", "Kon Tum", "Binh Dinh", "Ninh Thuan", "Dak Nong", 
            "Binh Thuan", "Phu Yen",
            "Quang Tri", "Da Nang", "Quang Ngai", "Gia Lai", "Khanh Hoa", "Lam Dong", "Dak Lak",
            
            // Các tỉnh miền Nam
            "Binh Duong", "Ba Ria Vung Tau", "Binh Phuoc", "Long An", "Soc Trang", "Hau Giang",
            "Ben Tre", "Tra Vinh", "Tien Giang", "Bac Lieu", "Kien Giang",
            "TP. Ho Chi Minh", "Dong Nai", "Tay Ninh", "Can Tho", "Vinh Long", "Dong Thap", 
            "Ca Mau", "An Giang",
            
            // Các tỉnh không tồn tại
            "Tinh Khong Ton Tai", "ABC", "XYZ"
        );

        System.out.println("Kết quả test hàm getMergedProvinceName:");
        System.out.println("========================================");
        System.out.printf("%-25s | %-25s | %-15s%n", "Tỉnh đầu vào", "Tỉnh sau sát nhập", "Vùng");
        System.out.println("----------------------------------------|-------------------------|----------------");

        for (String province : testProvinces) {
            String mergedProvince = config.getMergedProvinceName(province);
            String region = config.getRegion_groups().getRegionOfProvince(province);
            
            if (mergedProvince != null) {
                System.out.printf("%-25s | %-25s | %-15s%n", 
                    province, mergedProvince, region != null ? region : "N/A");
            } else {
                System.out.printf("%-25s | %-25s | %-15s%n", 
                    province, "KHÔNG TÌM THẤY", "N/A");
            }
        }

        System.out.println();
        System.out.println("=== THỐNG KÊ ===");
        
        // Thống kê theo vùng
        System.out.println("Số lượng tỉnh theo vùng:");
        if (config.getRegion_groups().getNorth() != null) {
            List<String> northProvinces = config.getRegion_groups().getProvincesByRegion("north");
            System.out.println("- Miền Bắc: " + (northProvinces != null ? northProvinces.size() : 0) + " tỉnh");
        }
        if (config.getRegion_groups().getCentral() != null) {
            List<String> centralProvinces = config.getRegion_groups().getProvincesByRegion("central");
            System.out.println("- Miền Trung: " + (centralProvinces != null ? centralProvinces.size() : 0) + " tỉnh");
        }
        if (config.getRegion_groups().getSouth() != null) {
            List<String> southProvinces = config.getRegion_groups().getProvincesByRegion("south");
            System.out.println("- Miền Nam: " + (southProvinces != null ? southProvinces.size() : 0) + " tỉnh");
        }

        // Test một số trường hợp đặc biệt
        System.out.println();
        System.out.println("=== TEST TRƯỜNG HỢP ĐẶC BIỆT ===");
        
        // Test với null
        System.out.println("Test với null: " + config.getMergedProvinceName(null));
        
        // Test với chuỗi rỗng
        System.out.println("Test với chuỗi rỗng: " + config.getMergedProvinceName(""));
        
        // Test với chuỗi chỉ có khoảng trắng
        System.out.println("Test với khoảng trắng: " + config.getMergedProvinceName("   "));
        
        // Test case insensitive
        System.out.println("Test case insensitive 'ha noi': " + config.getMergedProvinceName("ha noi"));
        System.out.println("Test case insensitive 'HA NOI': " + config.getMergedProvinceName("HA NOI"));
    }
} 