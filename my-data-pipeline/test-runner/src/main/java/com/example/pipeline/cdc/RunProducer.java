package com.example.pipeline.cdc;

import com.example.pipeline.CdcProducer;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.full.DeleteTopic;
import com.example.pipeline.service.TopicGenerator;
import java.util.List;
import java.util.Map;

public class RunProducer {
    public static void main(String[] args) {
        try {
            // Nếu truyền đủ args thì lấy từ args, không thì lấy từ config
            String kafkaBrokerSource;
            String producerHost;
            int producerPort;
            String producerNamespace;
            String producerSetName;
            int maxRetries;
            String consumerGroupList;
            int producerThreadPoolSize;
            int maxMessagesPerSecond;
            String topicList;

            if (args != null && args.length >= 9) {
                kafkaBrokerSource = args[0];
                producerHost = args[1];
                producerPort = Integer.parseInt(args[2]);
                producerNamespace = args[3];
                producerSetName = args[4];
                maxRetries = Integer.parseInt(args[5]);
                consumerGroupList = args[6];
                producerThreadPoolSize = Integer.parseInt(args[7]);
                maxMessagesPerSecond = Integer.parseInt(args[8]);
            } else {
                // Load configuration from config.yaml
                Config config = ConfigLoader.getConfig();
                if (config == null) {
                    throw new IllegalStateException("Khong the load cau hinh");
                }
                producerHost = config.getProducers().get(0).getHost();
                producerPort = config.getProducers().get(0).getPort();
                producerNamespace = config.getProducers().get(0).getNamespace();
                producerSetName = config.getProducers().get(0).getSet();
                kafkaBrokerSource = config.getKafka().getBroker();
                Map<String, List<String>> prefixMapping = config.getRegion_mapping();
                producerThreadPoolSize = 2;
                maxRetries = config.getPerformance().getMax_retries();
                maxMessagesPerSecond = (int) config.getPerformance().getRate_control().getMax_rate();
                // Tạo danh sách topic và consumer group từ prefix mapping
                StringBuilder topicListBuilder = new StringBuilder();
                StringBuilder consumerGroupListBuilder = new StringBuilder();
                for (Map.Entry<String, List<String>> entry : prefixMapping.entrySet()) {
                    String prefix = entry.getKey();
                    String producerName = config.getProducers().get(0).getName();
                    String baseTopic = TopicGenerator.TopicNameGenerator.generateTopicName(producerName, prefix);
                    String cdcTopic = TopicGenerator.generateCdcTopicName(baseTopic);
                    if (topicListBuilder.length() > 0) {
                        topicListBuilder.append(",");
                        consumerGroupListBuilder.append(",");
                    }
                    topicListBuilder.append(cdcTopic);
                    consumerGroupListBuilder.append(TopicGenerator.generateCdcGroupName(cdcTopic));
                }
                topicList = topicListBuilder.toString();
                consumerGroupList = consumerGroupListBuilder.toString();
            }

            // Xoa tat ca topic tu Kafka Source
            System.out.println("=== Xoa topic tu Kafka ===");
            System.out.println("Dang xoa topic tu Kafka Source: " + kafkaBrokerSource);
            DeleteTopic.deleteAllTopics(kafkaBrokerSource);
            System.out.println("Da xoa xong tat ca topic");
            System.out.println("=========================");

            System.out.println("\n=== Cau hinh chay ===");
            System.out.println("Kafka Broker Source: " + kafkaBrokerSource);
            System.out.println("Producer Host: " + producerHost);
            System.out.println("Producer Port: " + producerPort);
            System.out.println("Producer Namespace: " + producerNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("Producer Thread Pool Size: " + producerThreadPoolSize);
            System.out.println("Max Retries: " + maxRetries);
            System.out.println("Max Messages Per Second: " + maxMessagesPerSecond);
            System.out.println("=====================");

            // Gọi CDC Producer
            CdcProducer.main(new String[]{
                kafkaBrokerSource,
                producerHost,
                String.valueOf(producerPort),
                producerNamespace,
                producerSetName,
                String.valueOf(maxRetries),
                consumerGroupList,
                String.valueOf(producerThreadPoolSize),
                String.valueOf(maxMessagesPerSecond)
            });

        } catch (Exception e) {
            System.err.println("Loi: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 