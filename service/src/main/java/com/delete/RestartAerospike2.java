package com.delete;

import java.io.IOException;

public class RestartAerospike2 {
    public static void main(String[] args) {
        try {
            // Xóa file consumer.dat
            ProcessBuilder rmBuilder = new ProcessBuilder("sudo", "rm", "docker/aerospike_data/consumer.dat");
            Process rmProcess = rmBuilder.inheritIO().start();
            rmProcess.waitFor();

            // Restart container aerospike2
            ProcessBuilder restartBuilder = new ProcessBuilder("docker", "restart", "aerospike2");
            Process restartProcess = restartBuilder.inheritIO().start();
            restartProcess.waitFor();

            System.out.println("Đã xóa file và restart container aerospike2.");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
