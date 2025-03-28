package com.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class MainVertxApp {
    public static void main(String[] args) {
        // Tạo Vertx instance với số lượng event loop hợp lý
        Vertx vertx = Vertx.vertx(new VertxOptions().setEventLoopPoolSize(Runtime.getRuntime().availableProcessors() * 2));

        // Triển khai AerospikeToKafkaVerticle (tác vụ đọc)
        vertx.deployVerticle(new AerospikeToKafkaVerticle(vertx), res -> {
            if (res.succeeded()) {
                System.out.println("AerospikeToKafkaVerticle deployed successfully");
            } else {
                System.out.println("AerospikeToKafkaVerticle deployment failed!");
            }
        });

        // Triển khai KafkaToAerospikeVerticle (tác vụ ghi)
        vertx.deployVerticle(new KafkaToAerospikeVerticle(vertx), res -> {
            if (res.succeeded()) {
                System.out.println("KafkaToAerospikeVerticle deployed successfully");
            } else {
                System.out.println("KafkaToAerospikeVerticle deployment failed!");
            }
        });
    }
}
