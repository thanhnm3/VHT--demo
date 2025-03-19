package com.example;

import java.io.FileInputStream;
import java.io.IOException;

import example.Simple;

public class ReadFromFile {
    public static void main(String[] args) {
        try {
            FileInputStream input = new FileInputStream("person.bin");
            Simple.Person person = Simple.Person.parseFrom(input);
            input.close();

            System.out.println("Đọc từ file:");
            System.out.println("Tên: " + person.getName());
            System.out.println("Tuổi: " + person.getAge());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
