package com.example.pipeline;

import com.example.pipeline.full.MainAll;
import com.example.pipeline.cdc.Maincdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // Get mode from args or environment variable
        String mode = args.length > 0 ? args[0] : System.getenv("MODE");
        
        if (mode == null || mode.isEmpty()) {
            logger.error("Mode must be specified either as command line argument or MODE environment variable");
            System.exit(1);
        }

        logger.info("Starting application in mode: {}", mode);

        try {
            switch (mode.toLowerCase()) {
                case "all":
                    MainAll.main(args);
                    break;
                case "cdc":
                    Maincdc.main(args);
                    break;
                case "both":
                    // Start both modes in parallel using threads
                    Thread allThread = new Thread(() -> {
                        try {
                            MainAll.main(args);
                        } catch (Exception e) {
                            logger.error("Error in all mode: {}", e.getMessage(), e);
                        }
                    });
                    
                    Thread cdcThread = new Thread(() -> {
                        try {
                            Maincdc.main(args);
                        } catch (Exception e) {
                            logger.error("Error in cdc mode: {}", e.getMessage(), e);
                        }
                    });

                    allThread.start();
                    cdcThread.start();

                    // Wait for both threads to complete
                    allThread.join();
                    cdcThread.join();
                    break;
                default:
                    logger.error("Invalid mode: {}. Must be one of: all, cdc, both", mode);
                    System.exit(1);
            }
        } catch (Exception e) {
            logger.error("Error running application: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
} 