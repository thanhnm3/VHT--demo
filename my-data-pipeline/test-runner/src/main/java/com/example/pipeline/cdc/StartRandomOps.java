package com.example.pipeline.cdc;

import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartRandomOps {
    private static final Logger logger = LoggerFactory.getLogger(StartRandomOps.class);

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Lấy cấu hình từ producer đầu tiên
            Config.Producer producer = config.getProducers().get(0);
            
            // Hard code host và port
            String host = "localhost";
            int port = 3000;
            
            // Cấu hình performance
            int operationsPerSecond = 500; // Số lượng thao tác mỗi giây
            int threadPoolSize = 4; // Số thread cho RandomOperations

            logger.info("[RANDOM OPS] Starting with configuration:");
            logger.info("[RANDOM OPS] - Host: {}", host);
            logger.info("[RANDOM OPS] - Port: {}", port);
            logger.info("[RANDOM OPS] - Namespace: {}", producer.getNamespace());
            logger.info("[RANDOM OPS] - Set: {}", producer.getSet());
            logger.info("[RANDOM OPS] - Operations Per Second: {}", operationsPerSecond);
            logger.info("[RANDOM OPS] - Thread Pool Size: {}", threadPoolSize);

            // Khởi động RandomOperations
            RandomOperations.main(
                host,
                port,
                producer.getNamespace(),
                producer.getSet(),
                operationsPerSecond,
                threadPoolSize
            );

        } catch (Exception e) {
            logger.error("Critical error: {}", e.getMessage(), e);
        }
    }
} 