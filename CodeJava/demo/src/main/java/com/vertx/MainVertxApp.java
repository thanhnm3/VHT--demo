package com.vertx;

import io.vertx.core.Vertx;

public class MainVertxApp {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new AerospikeToKafkaVerticle(), res -> {
            if (res.succeeded()) {
                System.out.println("AerospikeToKafkaVerticle deployment id is: " + res.result());
            } else {
                System.out.println("AerospikeToKafkaVerticle deployment failed!");
            }
        });

        vertx.deployVerticle(new KafkaToAerospikeVerticle(), res -> {
            if (res.succeeded()) {
                System.out.println("KafkaToAerospikeVerticle deployment id is: " + res.result());
            } else {
                System.out.println("KafkaToAerospikeVerticle deployment failed!");
            }
        });
    }
}
