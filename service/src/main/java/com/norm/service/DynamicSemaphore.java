package com.norm.service;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class DynamicSemaphore {
    private final AtomicInteger currentPermits;
    private final int minPermits;
    private final int maxPermits;
    private final double targetCpuUsage;
    private final double targetMemoryUsage;
    private final ScheduledExecutorService scheduler;
    
    public DynamicSemaphore(int initialPermits, int minPermits, int maxPermits, 
                          double targetCpuUsage, double targetMemoryUsage) {
        this.currentPermits = new AtomicInteger(initialPermits);
        this.minPermits = minPermits;
        this.maxPermits = maxPermits;
        this.targetCpuUsage = targetCpuUsage;
        this.targetMemoryUsage = targetMemoryUsage;
        this.scheduler = Executors.newScheduledThreadPool(1);
        
        // Bắt đầu monitoring và điều chỉnh permits
        startMonitoring();
    }
    
    private void startMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                adjustPermits();
            } catch (Exception e) {
                System.err.println("Error adjusting permits: " + e.getMessage());
            }
        }, 0, 5, TimeUnit.SECONDS);
    }
    
    private void adjustPermits() {
        double cpuUsage = getCpuUsage();
        double memoryUsage = getMemoryUsage();
        int current = currentPermits.get();
        
        // Nếu CPU hoặc Memory usage cao, giảm permits
        if (cpuUsage > targetCpuUsage || memoryUsage > targetMemoryUsage) {
            int newPermits = Math.max(minPermits, current - 20);
            currentPermits.set(newPermits);
            System.out.printf("Reducing permits to %d due to high resource usage (CPU: %.2f%%, Memory: %.2f%%)%n",
                            newPermits, cpuUsage, memoryUsage);
        }
        // Nếu resource usage thấp, tăng permits
        else if (cpuUsage < targetCpuUsage * 0.8 && memoryUsage < targetMemoryUsage * 0.8) {
            int newPermits = Math.min(maxPermits, current + 100);
            currentPermits.set(newPermits);
            System.out.printf("Increasing permits to %d due to low resource usage (CPU: %.2f%%, Memory: %.2f%%)%n",
                            newPermits, cpuUsage, memoryUsage);
        }
        // Nếu permits quá thấp nhưng resource usage bình thường, tăng permits
        else if (current < minPermits * 2 && cpuUsage < targetCpuUsage * 0.9 && memoryUsage < targetMemoryUsage * 0.9) {
            int newPermits = Math.min(maxPermits, current + 50);
            currentPermits.set(newPermits);
            System.out.printf("Recovering permits to %d (CPU: %.2f%%, Memory: %.2f%%)%n",
                            newPermits, cpuUsage, memoryUsage);
        }
    }
    
    private double getCpuUsage() {
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            return ((com.sun.management.OperatingSystemMXBean) osBean).getCpuLoad() * 100;
        }
        return 0.0;
    }
    
    private double getMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        return (double) usedMemory / runtime.maxMemory() * 100;
    }
    
    public boolean tryAcquire() {
        return currentPermits.get() > 0 && currentPermits.decrementAndGet() >= 0;
    }
    
    public void acquire() throws InterruptedException {
        while (!tryAcquire()) {
            Thread.sleep(100); // Wait 100ms before retrying
        }
    }
    
    public void release() {
        currentPermits.incrementAndGet();
    }
    
    public int getCurrentPermits() {
        return currentPermits.get();
    }
    
    public void shutdown() {
        scheduler.shutdown();
    }
}
