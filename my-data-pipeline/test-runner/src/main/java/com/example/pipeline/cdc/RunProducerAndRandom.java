// package com.example.pipeline.cdc;

// import com.example.pipeline.CdcProducer;
// import com.example.pipeline.service.config.Config;
// import com.example.pipeline.service.ConfigLoader;
// import com.example.pipeline.full.DeleteTopic;


// public class RunProducerAndRandom {
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
//             String kafkaBrokerSource = config.getKafka().getBrokers().getSource();
//             String kafkaBrokerTarget = config.getKafka().getBrokers().getTarget();

//             // Cau hinh performance
//             int producerThreadPoolSize = 2; // So thread cho Producer
//             int randomOperationsThreadPoolSize = 4; // So thread cho RandomOperations
//             int maxMessagesPerSecond = config.getPerformance().getMax_messages_per_second();
//             int operationsPerSecond = 1000; // So luong thao tac moi giay cho RandomOperations
//             int maxRetries = config.getPerformance().getMax_retries();

//             // Xoa tat ca topic tu 2 kafka
//             System.out.println("=== Xoa topic tu Kafka ===");
//             System.out.println("Dang xoa topic tu Kafka Source: " + kafkaBrokerSource);
//             DeleteTopic.deleteAllTopics(kafkaBrokerSource);
//             System.out.println("Dang xoa topic tu Kafka Target: " + kafkaBrokerTarget);
//             DeleteTopic.deleteAllTopics(kafkaBrokerTarget);
//             System.out.println("Da xoa xong tat ca topic");
//             System.out.println("=========================");

//             System.out.println("\n=== Cau hinh chay ===");
//             System.out.println("Kafka Broker Source: " + kafkaBrokerSource);
//             System.out.println("Kafka Broker Target: " + kafkaBrokerTarget);
//             System.out.println("Producer Host: " + producerHost);
//             System.out.println("Producer Port: " + producerPort);
//             System.out.println("Producer Namespace: " + producerNamespace);
//             System.out.println("Producer Set Name: " + producerSetName);
//             System.out.println("Producer Thread Pool Size: " + producerThreadPoolSize);
//             System.out.println("Random Operations Thread Pool Size: " + randomOperationsThreadPoolSize);
//             System.out.println("Max Messages Per Second: " + maxMessagesPerSecond);
//             System.out.println("Operations Per Second: " + operationsPerSecond);
//             System.out.println("Max Retries: " + maxRetries);
//             System.out.println("=====================");

//             // Tao luong de chay AerospikeRandomOperations
//             Thread randomOperationsThread = new Thread(() -> {
//                 System.out.println("\n=== Bat dau Random Operations ===");
//                 RandomOperations.main(producerHost, producerPort, producerNamespace, producerSetName, 
//                     operationsPerSecond, randomOperationsThreadPoolSize);
//             });

//             // Tao luong de chay AerospikePoller
//             Thread cdcProducerThread = new Thread(() -> {
//                 System.out.println("\n=== Bat dau CdcProducer ===");
//                 CdcProducer.start(producerHost, producerPort, producerNamespace, producerSetName, 
//                     kafkaBrokerSource, maxRetries, producerThreadPoolSize);
//             });

//             // Bat dau cac luong
//             randomOperationsThread.start();
//             cdcProducerThread.start();

//             // Doi tat ca cac luong hoan thanh
//             try {
//                 randomOperationsThread.join();
//                 cdcProducerThread.join();
//             } catch (InterruptedException e) {
//                 System.err.println("Luong chinh bi ngat: " + e.getMessage());
//             }

//             System.out.println("\n=== Ket thuc chuong trinh ===");
//         } catch (Exception e) {
//             System.err.println("Loi: " + e.getMessage());
//             e.printStackTrace();
//         }
//     }
// } 