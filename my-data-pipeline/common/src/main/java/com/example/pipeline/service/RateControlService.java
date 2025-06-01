package com.example.pipeline.service;

import com.example.pipeline.service.config.Config;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RateControlService {
    private volatile double currentRate;        // Tốc độ hiện tại
    private volatile double targetRate;         // Tốc độ mục tiêu
    private final double maxRate;               // Tốc độ tối đa
    private final double minRate;               // Tốc độ tối thiểu
    private final int lagThreshold;             // Ngưỡng lag
    private final int monitoringIntervalSeconds; // Khoảng thời gian giám sát (giây)
    private final AtomicLong lastRateAdjustmentTime; // Thời điểm điều chỉnh tốc độ cuối cùng
    private final ScheduledExecutorService rateAdjustmentExecutor; // Executor để điều chỉnh tốc độ
    private final int rateAdjustmentSteps;      // Số bước điều chỉnh
    private final double maxRateChangePercent;  // Phần trăm thay đổi tốc độ tối đa mỗi bước

    public RateControlService() {
        Config config = ConfigLoader.getConfig();
        var rateControl = config.getPerformance().getRate_control();
        
        this.currentRate = rateControl.getInitial_rate();
        this.targetRate = rateControl.getInitial_rate();
        this.maxRate = rateControl.getMax_rate();
        this.minRate = rateControl.getMin_rate();
        this.lagThreshold = rateControl.getLag_threshold();
        this.monitoringIntervalSeconds = rateControl.getMonitoring_interval_seconds();
        this.lastRateAdjustmentTime = new AtomicLong(System.currentTimeMillis());
        this.rateAdjustmentExecutor = Executors.newSingleThreadScheduledExecutor();
        this.rateAdjustmentSteps = rateControl.getRate_adjustment_steps();
        this.maxRateChangePercent = rateControl.getMax_rate_change_percent();
    }

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

        // Lên lịch điều chỉnh tốc độ từng bước
        for (int i = 0; i < rateAdjustmentSteps; i++) {
            final int step = i;
            rateAdjustmentExecutor.schedule(() -> {
                double newRate = currentRate + finalStepSize;
                if (finalStepSize > 0) {
                    currentRate = Math.min(newRate, targetRate);
                } else {
                    currentRate = Math.max(newRate, targetRate);
                }
            }, step * 1000, TimeUnit.MILLISECONDS);
        }

        targetRate = newTargetRate;
    }


    public double calculateNewRateForProducer(long totalLag) {
        if (totalLag > lagThreshold) {
            // Nếu lag cao, giảm tốc độ xuống 10%
            return Math.max(minRate, currentRate * 0.9);
        } else if (totalLag < lagThreshold / 2) {
            // Nếu lag thấp, tăng tốc độ lên 10%
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

    public void updateRateForLag(long totalLag) {
        double newRate = calculateNewRateForProducer(totalLag);
        adjustRateSmoothly(newRate);
    }
} 