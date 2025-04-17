package com.cdc;



public class Maincdc {
    public static void main(String[] args) {
        int producerThreadPoolSize = 2; // Số lượng thread cho Producer
        int consumerThreadPoolSize = 4; // Số lượng thread cho Consumer
        int randomOperationsThreadPoolSize = 2; // Số lượng thread cho RandomOperations
        int maxMessagesPerSecond = 5000; // Giới hạn số lượng message mỗi giây
        int operationsPerSecond = 100; // Số lượng thao tác mỗi giây cho RandomOperations

        // Tạo luồng để chạy AerospikeRandomOperations
        Thread randomOperationsThread = new Thread(() -> {
            System.out.println("Starting AerospikeRandomOperations...");
            RandomOperations.main(new String[]{}, operationsPerSecond, randomOperationsThreadPoolSize); // Truyền tham số
        });

        // Tạo luồng để chạy AerospikePoller
        Thread pollerThread = new Thread(() -> {
            System.out.println("Starting CdcProducer...");
            CdcProducer.main(new String[]{}, producerThreadPoolSize); // Truyền threadPoolSize
        });

        // Tạo luồng để chạy CdcConsumer
        Thread consumerThread = new Thread(() -> {
            System.out.println("Starting CdcConsumer...");
            CdcConsumer.main(new String[]{}, consumerThreadPoolSize, maxMessagesPerSecond); // Truyền threadPoolSize và maxMessagesPerSecond
        });

        // Bắt đầu các luồng
        randomOperationsThread.start();
        pollerThread.start();
        consumerThread.start();

        // Đợi cả ba luồng hoàn thành (nếu cần)
        try {
            randomOperationsThread.join();
            pollerThread.join();
            consumerThread.join();
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted: " + e.getMessage());
        }

        System.out.println("All applications have finished execution.");
    }
}