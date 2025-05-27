package com.example.pipeline.cdc;

import com.example.pipeline.CdcConsumer;
import com.example.pipeline.CdcProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class Maincdc {
    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Failed to load configuration");
            }

            // Lấy cấu hình Producer
            String producerHost = config.getProducers().get(0).getHost();
            int producerPort = config.getProducers().get(0).getPort();
            String producerNamespace = config.getProducers().get(0).getNamespace();
            String producerSetName = config.getProducers().get(0).getSet();
            String kafkaBroker = config.getKafka().getBrokers().getSource();

            // Lấy cấu hình Consumer cho CDC từ prefix mapping
            Map<String, List<String>> prefixMapping = config.getPrefix_mapping();
            List<Config.Consumer> cdcConsumers = new ArrayList<>();
            List<Thread> consumerThreads = new ArrayList<>();

            // Tạo consumer cho mỗi prefix
            for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                String prefix = entry.getKey();
                String consumerName = entry.getValue().get(0);
                
                Config.Consumer consumer = config.getConsumers().stream()
                    .filter(c -> c.getName().equals(consumerName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                        String.format("No consumer config found for prefix %s (consumer: %s)", 
                        prefix, consumerName)));
                
                cdcConsumers.add(consumer);
            }

            // Cấu hình performance
            int producerThreadPoolSize = 2; // Số thread cho Producer
            int consumerThreadPoolSize = 2; // Số thread cho Consumer
            int randomOperationsThreadPoolSize = 4; // Số thread cho RandomOperations
            int maxMessagesPerSecond = config.getPerformance().getMax_messages_per_second();
            int operationsPerSecond = 500; // Số lượng thao tác mỗi giây cho RandomOperations
            int maxRetries = config.getPerformance().getMax_retries();

            System.out.println("=== Starting CDC Pipeline ===");
            System.out.println("Kafka Broker: " + kafkaBroker);
            System.out.println("Producer Host: " + producerHost);
            System.out.println("Producer Port: " + producerPort);
            System.out.println("Producer Namespace: " + producerNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            
            // In thông tin cấu hình cho mỗi consumer
            for (Config.Consumer consumer : cdcConsumers) {
                System.out.println("Consumer " + consumer.getName() + " Host: " + consumer.getHost());
                System.out.println("Consumer " + consumer.getName() + " Port: " + consumer.getPort());
                System.out.println("Consumer " + consumer.getName() + " Namespace: " + consumer.getNamespace());
            }
            
            System.out.println("Producer Thread Pool Size: " + producerThreadPoolSize);
            System.out.println("Consumer Thread Pool Size: " + consumerThreadPoolSize);
            System.out.println("Random Operations Thread Pool Size: " + randomOperationsThreadPoolSize);
            System.out.println("Max Messages Per Second: " + maxMessagesPerSecond);
            System.out.println("Operations Per Second: " + operationsPerSecond);
            System.out.println("Max Retries: " + maxRetries);
            System.out.println("===========================");

            // Tạo luồng để chạy AerospikeRandomOperations
            Thread randomOperationsThread = new Thread(() -> {
                System.out.println("Starting AerospikeRandomOperations...");
                RandomOperations.main(producerHost, producerPort, producerNamespace, producerSetName, 
                    operationsPerSecond, randomOperationsThreadPoolSize);
            });

            // Tạo luồng để chạy AerospikePoller
            Thread cdcProducerThread = new Thread(() -> {
                System.out.println("Starting CdcProducer...");
                CdcProducer.start(producerHost, producerPort, producerNamespace, producerSetName, 
                    kafkaBroker, maxRetries, producerThreadPoolSize);
            });

            // Tạo luồng cho mỗi consumer
            for (Config.Consumer consumer : cdcConsumers) {
                Thread consumerThread = new Thread(() -> {
                    System.out.println("Starting CdcConsumer for " + consumer.getName() + "...");
                    CdcConsumer.main(new String[]{}, consumerThreadPoolSize, maxMessagesPerSecond,
                        producerHost, producerPort, producerNamespace,
                        consumer.getHost(), consumer.getPort(), kafkaBroker);
                });
                consumerThreads.add(consumerThread);
            }

            // Bắt đầu các luồng
            randomOperationsThread.start();
            cdcProducerThread.start();
            for (Thread thread : consumerThreads) {
                thread.start();
            }

            // Đợi tất cả các luồng hoàn thành
            try {
                randomOperationsThread.join();
                for (Thread thread : consumerThreads) {
                    thread.join();
                }
                cdcProducerThread.join();
            } catch (InterruptedException e) {
                System.err.println("Main thread interrupted: " + e.getMessage());
            }

            System.out.println("All applications have finished execution.");
        } catch (Exception e) {
            System.err.println("Critical error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}