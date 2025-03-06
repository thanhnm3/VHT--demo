package com.example;

import example.Simple; // Import file protobuf đã tạo

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Random;


public class Main {
    private static final String FILE_NAME = "people_data.bin"; // File lưu dữ liệu

    public static void main(String[] args) {
        // Ghi dữ liệu random vào file
        writeRandomPeopleToFile(5); // Ghi 5 người
        // Đọc dữ liệu từ file và hiển thị ra màn hình
        readPeopleFromFile();
    }

    // Hàm tạo dữ liệu ngẫu nhiên và ghi vào file
    public static void writeRandomPeopleToFile(int count) {
        Random random = new Random();

        try (FileOutputStream outputStream = new FileOutputStream(FILE_NAME)) {
            for (int i = 0; i < count; i++) {
                Simple.Person person = Simple.Person.newBuilder()
                        .setName("Person_" + (i + 1))
                        .setAge(random.nextInt(50) + 18) // Tuổi từ 18 - 67
                        .build();

                // Ghi từng đối tượng vào file
                person.writeDelimitedTo(outputStream);
            }
            System.out.println("Ghi thành công " + count + " người vào file " + FILE_NAME);
        } catch (IOException e) {
            System.err.println("Lỗi khi ghi file: " + e.getMessage());
        }
    }

    // Hàm đọc dữ liệu từ file và hiển thị ra màn hình
    public static void readPeopleFromFile() {
        try (FileInputStream inputStream = new FileInputStream(FILE_NAME)) {
            System.out.println("\n Dữ liệu từ file:");
            while (inputStream.available() > 0) {
                Simple.Person person = Simple.Person.parseDelimitedFrom(inputStream);
                System.out.println("Tên: " + person.getName() + ", Tuổi: " + person.getAge());
            }
        } catch (IOException e) {
            System.err.println("Lỗi khi đọc file: " + e.getMessage());
        }
    }
}
