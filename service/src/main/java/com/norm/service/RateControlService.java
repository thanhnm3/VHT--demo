package com.norm.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RateControlService {
    private volatile double currentRate;
    private volatile double targetRate;
    private final double maxRate;
    private final double minRate;
    private final int lagThreshold;
    private final int monitoringIntervalSeconds;
    private final AtomicLong lastRateAdjustmentTime;
    private final ScheduledExecutorService rateAdjustmentExecutor;
    private final int rateAdjustmentSteps;
    private final double maxRateChangePercent;

    public RateControlService(double initialRate, double maxRate, double minRate, 
                            int lagThreshold, int monitoringIntervalSeconds) {
        this.currentRate = initialRate;
        this.targetRate = initialRate;
        this.maxRate = maxRate;
        this.minRate = minRate;
        this.lagThreshold = lagThreshold;
        this.monitoringIntervalSeconds = monitoringIntervalSeconds;
        this.lastRateAdjustmentTime = new AtomicLong(System.currentTimeMillis());
        this.rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();
        this.rateAdjustmentSteps = 5;
        this.maxRateChangePercent = 0.2;
    }

    public double getCurrentRate() {
        return currentRate;
    }

    public double getTargetRate() {
        return targetRate;
    }

    public void adjustRateSmoothly(double newTargetRate) {
        if (newTargetRate == targetRate) return;

        // Tính toán số bước và lượng thay đổi cho mỗi bước
        final double rateChange = newTargetRate - currentRate;
        final double stepSize = rateChange / rateAdjustmentSteps;
        
        // Giới hạn thay đổi tối đa mỗi bước
        final double maxStepChange = currentRate * maxRateChangePercent;
        final double finalStepSize = Math.abs(stepSize) > maxStepChange ? 
            Math.signum(stepSize) * maxStepChange : stepSize;

        // Lên lịch điều chỉnh rate từng bước
        for (int i = 0; i < rateAdjustmentSteps; i++) {
            final int step = i;
            rateAdjustmentExecutor.schedule(() -> {
                double newRate = currentRate + finalStepSize;
                if (finalStepSize > 0) {
                    currentRate = Math.min(newRate, targetRate);
                } else {
                    currentRate = Math.max(newRate, targetRate);
                }
                System.out.printf("Rate adjusted to %.2f messages/second%n", currentRate);
            }, step * 1000, TimeUnit.MILLISECONDS);
        }

        targetRate = newTargetRate;
    }

    public double calculateNewRateForConsumer(long currentOffset, long lastProcessedOffset) {
        long lag = currentOffset - lastProcessedOffset;
        if (lag > lagThreshold) {
            return Math.min(maxRate, currentRate * 1.2);
        } else if (lag < lagThreshold / 2) {
            return Math.max(minRate, currentRate * 0.9);
        }
        return currentRate;
    }

    public double calculateNewRateForProducer(long totalLag) {
        if (totalLag > lagThreshold) {
            return Math.max(minRate, currentRate * 0.9);
        } else if (totalLag < lagThreshold / 2) {
            return Math.min(maxRate, currentRate * 1.1);
        }
        return currentRate;
    }

    public void updateRate(double newRate) {
        adjustRateSmoothly(newRate);
    }

    public boolean shouldCheckRateAdjustment() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastRateAdjustmentTime.get() >= monitoringIntervalSeconds * 1000) {
            lastRateAdjustmentTime.set(currentTime);
            return true;
        }
        return false;
    }

    public void shutdown() {
        rateAdjustmentExecutor.shutdown();
        try {
            if (!rateAdjustmentExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                rateAdjustmentExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            rateAdjustmentExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
} 