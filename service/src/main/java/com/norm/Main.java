package com.norm;

public class Main {

    public static void main(String[] args) {
        // Cấu hình worker pool size
        final int workerPoolSizeProducer = 2; // Số lượng thread trong Thread Pool
        final int workerPoolSizeConsumer = 4; // Số lượng thread trong Thread Pool cho Aerospike

        // Khởi chạy KafkaToAerospike trong một luồng riêng
        Thread producer = new Thread(() -> {
            try {
                AProducer.main(args, workerPoolSizeProducer);
            } catch (Exception e) {
                System.err.println("Error in KafkaToAerospike: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Khởi chạy AerospikeToKafka trong một luồng riêng
        Thread consumer = new Thread(() -> {
            try {
                AConsumer.main(args, workerPoolSizeConsumer);
            } catch (Exception e) {
                System.err.println("Error in AerospikeToKafka: " + e.getMessage());
                e.printStackTrace();
            }
        });

        // Bắt đầu cả hai luồng
        producer.start();
        consumer.start();

        // Đợi cả hai luồng hoàn thành (nếu cần)
        try {
            producer.join();
            consumer.join();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
