// package com.example.pipeline.model;

// import com.google.common.util.concurrent.RateLimiter;
// import com.example.pipeline.service.RateControlService;
// import com.example.pipeline.service.KafkaConsumerService;
// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import java.util.concurrent.ThreadPoolExecutor;
// import java.util.concurrent.atomic.AtomicInteger;
// import java.util.concurrent.ThreadFactory;
// import java.util.concurrent.LinkedBlockingQueue;
// import java.util.concurrent.TimeUnit;
// import java.util.Collections;
// import java.util.Map;
// import com.example.pipeline.service.config.ConfigurationService;

// public class ConsumerMetrics {
//     private volatile double currentRate;
//     private final RateLimiter rateLimiter;
//     private final RateControlService rateControlService;
//     private final KafkaConsumerService kafkaService;
//     private final KafkaConsumer<byte[], byte[]> consumer;
//     private final ThreadPoolExecutor workers;
//     private final String targetNamespace;
//     private final String prefix;
//     private final String setName;
//     private final int workerPoolSize;
//     private volatile boolean isRunning = true;

//     public ConsumerMetrics(String sourceNamespace, String kafkaBroker, 
//                    String consumerGroup, String targetNamespace,
//                    String prefix, String setName, int workerPoolSize,
//                    Map<String, String> prefixToTopicMap) {
//         this.currentRate = 8000.0;
//         this.rateLimiter = RateLimiter.create(currentRate);
//         this.rateControlService = new RateControlService(currentRate, 10000.0, 2000.0, 
//                                                        1000, 10);
//         this.workerPoolSize = workerPoolSize;
        
//         // Get topic from prefix mapping
//         String topic = prefixToTopicMap.get(prefix);
//         if (topic == null) {
//             throw new IllegalArgumentException("No topic mapping found for prefix: " + prefix);
//         }
        
//         String mirroredTopic = "source-kafka." + topic;
//         this.kafkaService = new KafkaConsumerService(kafkaBroker, ConfigurationService.getInstance());
//         this.consumer = this.kafkaService.createConsumer(mirroredTopic, consumerGroup);
//         this.consumer.subscribe(Collections.singletonList(mirroredTopic));
//         this.targetNamespace = targetNamespace;
//         this.prefix = prefix;
//         this.setName = setName;
        
//         // Create worker pool with CallerRunsPolicy
//         this.workers = new ThreadPoolExecutor(
//             workerPoolSize,
//             workerPoolSize,
//             60L, TimeUnit.SECONDS,
//             new LinkedBlockingQueue<>(1000),
//             new ThreadFactory() {
//                 private final AtomicInteger threadCount = new AtomicInteger(1);
//                 @Override
//                 public Thread newThread(Runnable r) {
//                     Thread thread = new Thread(r);
//                     thread.setName(prefix + "-worker-" + threadCount.getAndIncrement());
//                     return thread;
//                 }
//             },
//             new ThreadPoolExecutor.CallerRunsPolicy()
//         );
//     }

//     public void monitorAndAdjustLag() {
//         double newRate = rateControlService.calculateNewRateForConsumer(
//             kafkaService.getCurrentOffset(), 
//             kafkaService.getLastProcessedOffset()
//         );
//         rateControlService.updateRate(newRate);
//         currentRate = rateControlService.getCurrentRate();
//         rateLimiter.setRate(currentRate);
//     }

//     public void shutdown() {
//         isRunning = false;
//         if (rateControlService != null) {
//             rateControlService.shutdown();
//         }
//         if (kafkaService != null) {
//             kafkaService.shutdown();
//         }
//         if (consumer != null) {
//             consumer.wakeup();
//             consumer.close();
//         }
//         if (workers != null) {
//             workers.shutdown();
//             try {
//                 if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
//                     workers.shutdownNow();
//                 }
//             } catch (InterruptedException e) {
//                 workers.shutdownNow();
//             }
//         }
//     }

//     // Getters
//     public double getCurrentRate() {
//         return currentRate;
//     }

//     public RateLimiter getRateLimiter() {
//         return rateLimiter;
//     }

//     public RateControlService getRateControlService() {
//         return rateControlService;
//     }

//     public KafkaConsumer<byte[], byte[]> getConsumer() {
//         return consumer;
//     }

//     public ThreadPoolExecutor getWorkers() {
//         return workers;
//     }

//     public String getTargetNamespace() {
//         return targetNamespace;
//     }

//     public String getPrefix() {
//         return prefix;
//     }

//     public String getSetName() {
//         return setName;
//     }

//     public boolean isRunning() {
//         return isRunning;
//     }

//     public int getWorkerPoolSize() {
//         return workerPoolSize;
//     }
// } 