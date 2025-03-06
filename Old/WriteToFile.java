package com.example;
import java.io.FileOutputStream;
import java.io.IOException;

import example.Simple;

public class WriteToFile {
    public static void main(String[] args) {
        try {
            Simple.Person person = Simple.Person.newBuilder()
                    .setName("Nguyen Van A")
                    .setAge(25)
                    .build();

            FileOutputStream output = new FileOutputStream("person.bin");
            person.writeTo(output);
            output.close();

            System.out.println("Đã ghi dữ liệu vào person.bin");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
