package com.example.pipeline.testmm2;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Properties;
import java.util.Set;
import java.util.Map;
import java.util.Collections;

public class TopicCheck {
    private static final String SOURCE_BROKER = "localhost:9092";
    private static final String TARGET_BROKER = "localhost:9093";
    private static final String SOURCE_CLUSTER_NAME = "source-kafka";
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 5000; // 5 seconds
    private static final int REQUEST_TIMEOUT_MS = 30000; // 30 seconds
    private static final String TEST_TOPIC = "test-connection-topic";
    
    private final AdminClient sourceAdminClient;
    private final AdminClient targetAdminClient;

    public TopicCheck() {
        // Initialize source Kafka admin client
        Properties sourceProps = new Properties();
        sourceProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, SOURCE_BROKER);
        sourceProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        sourceProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        sourceAdminClient = AdminClient.create(sourceProps);

        // Initialize target Kafka admin client
        Properties targetProps = new Properties();
        targetProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TARGET_BROKER);
        targetProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        targetProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        targetAdminClient = AdminClient.create(targetProps);
    }

    public void testConnection() {
        System.out.println("\n=== KIEM TRA KET NOI ===");
        
        // Test source Kafka
        System.out.println("\n1. Kiem tra ket noi toi Source Kafka (" + SOURCE_BROKER + "):");
        try {
            // Tạo topic test
            NewTopic newTopic = new NewTopic(TEST_TOPIC, 1, (short) 1);
            sourceAdminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("✓ Tao topic thanh cong tren Source Kafka");
            
            // Xóa topic test
            sourceAdminClient.deleteTopics(Collections.singleton(TEST_TOPIC)).all().get();
            System.out.println("✓ Xoa topic thanh cong tren Source Kafka");
            
            System.out.println("✓ Ket noi toi Source Kafka thanh cong!");
        } catch (Exception e) {
            System.err.println("✗ Loi khi ket noi toi Source Kafka:");
            System.err.println("  Message: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("  Nguyen nhan: " + e.getCause().getMessage());
            }
        }

        // Test target Kafka
        System.out.println("\n2. Kiem tra ket noi toi Target Kafka (" + TARGET_BROKER + "):");
        try {
            // Tạo topic test
            NewTopic newTopic = new NewTopic(TEST_TOPIC, 1, (short) 1);
            targetAdminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("✓ Tao topic thanh cong tren Target Kafka");
            
            // Xóa topic test
            targetAdminClient.deleteTopics(Collections.singleton(TEST_TOPIC)).all().get();
            System.out.println("✓ Xoa topic thanh cong tren Target Kafka");
            
            System.out.println("✓ Ket noi toi Target Kafka thanh cong!");
        } catch (Exception e) {
            System.err.println("✗ Loi khi ket noi toi Target Kafka:");
            System.err.println("  Message: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("  Nguyen nhan: " + e.getCause().getMessage());
            }
        }
    }

    private Set<String> getTopicsWithRetry(AdminClient client, String brokerName) throws Exception {
        int retries = 0;
        Exception lastException = null;

        while (retries < MAX_RETRIES) {
            try {
                System.out.println("Dang ket noi toi " + brokerName + " (lan thu " + (retries + 1) + ")...");
                ListTopicsResult listTopicsResult = client.listTopics();
                return listTopicsResult.names().get();
            } catch (Exception e) {
                lastException = e;
                System.out.println("Loi khi ket noi toi " + brokerName + ": " + e.getMessage());
                if (retries < MAX_RETRIES - 1) {
                    System.out.println("Thu lai sau " + (RETRY_DELAY_MS/1000) + " giay...");
                    Thread.sleep(RETRY_DELAY_MS);
                }
                retries++;
            }
        }
        throw new Exception("Khong the ket noi toi " + brokerName + " sau " + MAX_RETRIES + " lan thu", lastException);
    }

    public Set<String> getSourceTopics() throws Exception {
        return getTopicsWithRetry(sourceAdminClient, "Source Kafka (" + SOURCE_BROKER + ")");
    }

    public Set<String> getTargetTopics() throws Exception {
        return getTopicsWithRetry(targetAdminClient, "Target Kafka (" + TARGET_BROKER + ")");
    }

    public void checkMirrorStatus() {
        try {
            System.out.println("=== KIEM TRA TRANG THAI MIRROR TOPICS ===");
            System.out.println("Ket noi toi source Kafka: " + SOURCE_BROKER);
            System.out.println("Ket noi toi target Kafka: " + TARGET_BROKER);
            
            Set<String> sourceTopics = getSourceTopics();
            Set<String> targetTopics = getTargetTopics();
            
            // Lọc ra các topic ứng dụng (loại bỏ các topic hệ thống)
            Set<String> applicationTopics = sourceTopics.stream()
                    .filter(topic -> !topic.startsWith("__") && !topic.startsWith("mm2-") && !topic.equals("heartbeats"))
                    .collect(java.util.stream.Collectors.toSet());

            System.out.println("\nDanh sach topic trong source Kafka:");
            applicationTopics.forEach(topic -> System.out.println("- " + topic));

            System.out.println("\nDanh sach topic trong target Kafka:");
            targetTopics.forEach(topic -> System.out.println("- " + topic));

            for (String topic : applicationTopics) {
                String mirroredTopic = SOURCE_CLUSTER_NAME + "." + topic; // Format: source-kafka.topic
                boolean isMirrored = targetTopics.contains(mirroredTopic);
                
                System.out.println("\nTopic: " + topic);
                System.out.println("Mirrored topic name expected: " + mirroredTopic);
                System.out.println("Trang thai mirror: " + (isMirrored ? "DA MIRROR" : "CHUA MIRROR"));
                
                if (isMirrored) {
                    // Lấy thông tin chi tiết về topic đã mirror
                    DescribeTopicsResult describeResult = targetAdminClient.describeTopics(Set.of(mirroredTopic));
                    Map<String, org.apache.kafka.common.KafkaFuture<TopicDescription>> topicFutures = describeResult.topicNameValues();
                    TopicDescription description = topicFutures.get(mirroredTopic).get();
                    
                    System.out.println("Số partition: " + description.partitions().size());
                    System.out.println("Replication factor: " + description.partitions().get(0).replicas().size());
                } else {
                    // Kiểm tra xem có topic nào trong target có chứa tên topic gốc không
                    System.out.println("Các topic trong target có thể liên quan:");
                    targetTopics.stream()
                        .filter(t -> t.contains(topic))
                        .forEach(t -> System.out.println("- " + t));
                }
            }

            // Kiểm tra các topic không được mirror
            Set<String> notMirroredTopics = applicationTopics.stream()
                    .filter(topic -> !targetTopics.contains(SOURCE_CLUSTER_NAME + "." + topic))
                    .collect(java.util.stream.Collectors.toSet());

            if (!notMirroredTopics.isEmpty()) {
                System.out.println("\n=== CAC TOPIC CHUA DUOC MIRROR ===");
                notMirroredTopics.forEach(topic -> System.out.println("- " + topic));
            }

        } catch (Exception e) {
            System.err.println("\nLoi khi kiem tra trang thai mirror:");
            System.err.println("Message: " + e.getMessage());
            if (e.getCause() != null) {
                System.err.println("Nguyen nhan: " + e.getCause().getMessage());
            }
            e.printStackTrace();
        }
    }

    public void compareTopics() {
        try {
            Set<String> sourceTopics = getSourceTopics();
            Set<String> targetTopics = getTargetTopics();

            System.out.println("Source Kafka Topics:");
            sourceTopics.forEach(System.out::println);

            System.out.println("\nTarget Kafka Topics:");
            targetTopics.forEach(System.out::println);

            System.out.println("\nTopics in source but not in target:");
            sourceTopics.stream()
                    .filter(topic -> !targetTopics.contains(topic))
                    .forEach(System.out::println);

            System.out.println("\nTopics in target but not in source:");
            targetTopics.stream()
                    .filter(topic -> !sourceTopics.contains(topic))
                    .forEach(System.out::println);

        } catch (Exception e) {
            System.err.println("Error comparing topics: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void close() {
        if (sourceAdminClient != null) {
            sourceAdminClient.close();
        }
        if (targetAdminClient != null) {
            targetAdminClient.close();
        }
    }

    public static void main(String[] args) {
        TopicCheck checker = new TopicCheck();
        try {
            checker.testConnection(); // Thêm dòng này để test kết nối trước
            checker.checkMirrorStatus();
        } finally {
            checker.close();
        }
    }
}
