package com.cdc;



public class Maincdc {
    public static void main(String[] args) {
        // Tạo luồng để chạy AerospikeRandomOperations
        Thread randomOperationsThread = new Thread(() -> {
            System.out.println("Starting AerospikeRandomOperations...");
            RandomOperations.main(new String[]{}); // Khởi chạy ứng dụng
        });

        // Tạo luồng để chạy AerospikePoller
        Thread pollerThread = new Thread(() -> {
            System.out.println("Starting AerospikePoller...");
            CdcProducer.main(new String[]{}); // Khởi chạy ứng dụng
        });

        // Tạo luồng để chạy CdcConsumer
        Thread consumerThread = new Thread(() -> {
            System.out.println("Starting CdcConsumer...");
            CdcConsumer.main(new String[]{}, 1, 5000); 
            System.out.println("==========================="); // In ra dấu phân cách sau mỗi lần chạy
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