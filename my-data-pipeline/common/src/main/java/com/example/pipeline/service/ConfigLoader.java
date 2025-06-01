package com.example.pipeline.service;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.LoaderOptions;
import com.example.pipeline.service.config.Config;

import java.io.InputStream;

public class ConfigLoader {
    private static Config config;

    static {
        // Sử dụng LoaderOptions để khởi tạo Constructor
        LoaderOptions options = new LoaderOptions();
        Constructor constructor = new Constructor(Config.class, options);
        Yaml yaml = new Yaml(constructor);

        try (InputStream inputStream = ConfigLoader.class.getClassLoader().getResourceAsStream("config.yaml")) {
            if (inputStream == null) {
                throw new IllegalArgumentException("File config.yaml không tồn tại trong resources");
            }
            config = yaml.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Trả về đối tượng Config
    public static Config getConfig() {
        return config;
    }
}