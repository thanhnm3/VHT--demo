// package com.example.pipeline.cdc;

// import com.example.pipeline.service.config.Config;
// import com.example.pipeline.RandomOperations;
// import com.example.pipeline.service.ConfigLoader;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// public class RunRandomOperations {
//     private static final Logger logger = LoggerFactory.getLogger(RunRandomOperations.class);

//     public static void main(String[] args) {
//         try {
//             // Load configuration from config.yaml
//             Config config = ConfigLoader.getConfig();
//             if (config == null) {
//                 throw new IllegalStateException("Khong the load cau hinh");
//             }

//             // Lay cau hinh Producer
//             String producerHost = config.getProducers().get(0).getHost();
//             int producerPort = config.getProducers().get(0).getPort();
//             String producerNamespace = config.getProducers().get(0).getNamespace();
//             String producerSetName = config.getProducers().get(0).getSet();

//             // Cau hinh performance
//             int randomOperationsThreadPoolSize = 4; // So thread cho RandomOperations
//             int operationsPerSecond = 2000; // So luong thao tac moi giay cho RandomOperations

//             logger.info("=== Bat dau Random Operations ===");
//             logger.info("Cau hinh Aerospike:");
//             logger.info("- Host: {}", producerHost);
//             logger.info("- Port: {}", producerPort);
//             logger.info("- Namespace: {}", producerNamespace);
//             logger.info("- Set Name: {}", producerSetName);
            
//             logger.info("\nCau hinh Performance:");
//             logger.info("- Thread Pool Size: {}", randomOperationsThreadPoolSize);
//             logger.info("- Operations Per Second: {}", operationsPerSecond);
            
//             logger.info("\nGioi han thao tac theo region:");
//             logger.info("North:");
//             logger.info("- Insert: 1000");
//             logger.info("- Update: 1000");
//             logger.info("- Delete: 1000");
//             logger.info("Central:");
//             logger.info("- Insert: 1000");
//             logger.info("- Update: 1000");
//             logger.info("- Delete: 1000");
//             logger.info("South:");
//             logger.info("- Insert: 1000");
//             logger.info("- Update: 1000");
//             logger.info("- Delete: 1000");
//             logger.info("===============================");

//             // Chay RandomOperations
//             RandomOperations.main(
//                 producerHost, 
//                 producerPort, 
//                 producerNamespace, 
//                 producerSetName, 
//                 operationsPerSecond, 
//                 randomOperationsThreadPoolSize
//             );

//         } catch (Exception e) {
//             logger.error("Loi khi chay Random Operations: {}", e.getMessage(), e);
//         }
//     }
// } 