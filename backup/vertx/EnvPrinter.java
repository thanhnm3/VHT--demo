package com.vertx;

import io.github.cdimascio.dotenv.Dotenv;

public class EnvPrinter {
    public static void main(String[] args) {
        // Load cấu hình từ file .env
        Dotenv dotenv = Dotenv.configure()
                              .directory("service//.env") // Đường dẫn tới file .env
                              .load();

        // In ra các giá trị từ file .env
        System.out.println("AEROSPIKE_PRODUCER_HOST: " + dotenv.get("AEROSPIKE_PRODUCER_HOST"));
        System.out.println("AEROSPIKE_PRODUCER_PORT: " + dotenv.get("AEROSPIKE_PRODUCER_PORT"));
        System.out.println("AEROSPIKE_CONSUMER_HOST: " + dotenv.get("AEROSPIKE_CONSUMER_HOST"));
        System.out.println("AEROSPIKE_CONSUMER_PORT: " + dotenv.get("AEROSPIKE_CONSUMER_PORT"));
        System.out.println("PRODUCER_NAMESPACE: " + dotenv.get("PRODUCER_NAMESPACE"));
        System.out.println("CONSUMER_NAMESPACE: " + dotenv.get("CONSUMER_NAMESPACE"));
        System.out.println("CONSUMER_GROUP: " + dotenv.get("CONSUMER_GROUP"));
        System.out.println("PRODUCER_SET_NAME: " + dotenv.get("PRODUCER_SET_NAME"));
        System.out.println("CONSUMER_SET_NAME: " + dotenv.get("CONSUMER_SET_NAME"));
        System.out.println("KAFKA_BROKER: " + dotenv.get("KAFKA_BROKER"));
        System.out.println("KAFKA_TOPIC: " + dotenv.get("KAFKA_TOPIC"));
        System.out.println("MAX_MESSAGES_PER_SECOND: " + dotenv.get("MAX_MESSAGES_PER_SECOND"));
    }
}