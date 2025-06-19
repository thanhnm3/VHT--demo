package com.example.pipeline.service;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.LoaderOptions;
import com.example.pipeline.service.config.Config;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;

public class ConfigLoader {
    private static Config config;

    static {
        loadConfig();
    }

    private static void loadConfig() {
        // Sử dụng LoaderOptions để khởi tạo Constructor
        LoaderOptions options = new LoaderOptions();
        Constructor constructor = new Constructor(Config.class, options);
        Yaml yaml = new Yaml(constructor);

        try {
            // Kiểm tra environment variable CONFIG_FILE trước
            String configFile = System.getenv("CONFIG_FILE");
            InputStream inputStream = null;

            if (configFile != null && !configFile.trim().isEmpty()) {
                // Thử đọc từ file system trước
                File file = new File(configFile);
                if (file.exists()) {
                    inputStream = new FileInputStream(file);
                    System.out.println("Loading config from file: " + configFile);
                } else {
                    // Thử đọc từ classpath
                    inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream(configFile);
                    if (inputStream != null) {
                        System.out.println("Loading config from classpath: " + configFile);
                    }
                }
            }

            // Nếu không có CONFIG_FILE hoặc không tìm thấy, dùng config.yaml mặc định
            if (inputStream == null) {
                inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream("config.yaml");
                if (inputStream == null) {
                    throw new IllegalArgumentException("File config.yaml không tồn tại trong resources");
                }
                System.out.println("Loading default config: config.yaml");
            }

            config = yaml.load(inputStream);
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Trả về đối tượng Config
    public static Config getConfig() {
        return config;
    }

    // Reload config (hữu ích khi muốn thay đổi config runtime)
    public static void reloadConfig() {
        loadConfig();
    }
}