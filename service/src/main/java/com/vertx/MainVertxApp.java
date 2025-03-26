package com.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class MainVertxApp {
    public static void main(String[] args) {
        // Tạo Vertx instance cho tác vụ đọc (ít threads hơn)
        Vertx vertx1 = Vertx.vertx(new VertxOptions().setWorkerPoolSize(Runtime.getRuntime().availableProcessors() * 2));
        vertx1.deployVerticle(new AerospikeToKafkaVerticle(vertx1), res -> { // Truyền vertx1 vào constructor
            if (res.succeeded()) {
                System.out.println("AerospikeToKafkaVerticle deployment id is: " + res.result());
            } else {
                System.out.println("AerospikeToKafkaVerticle deployment failed!");
            }
        });

        // Tạo Vertx instance cho tác vụ ghi (nhiều threads hơn)
        Vertx vertx2 = Vertx.vertx(new VertxOptions().setWorkerPoolSize(Runtime.getRuntime().availableProcessors() * 8));
        vertx2.deployVerticle(new KafkaToAerospikeVerticle(vertx2), res -> { // Truyền vertx2 vào constructor
            if (res.succeeded()) {
                System.out.println("KafkaToAerospikeVerticle deployment id is: " + res.result());
            } else {
                System.out.println("KafkaToAerospikeVerticle deployment failed!");
            }
        });
    }
}
