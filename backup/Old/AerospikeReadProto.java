package com.example;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.google.protobuf.InvalidProtocolBufferException;

import example.Simple; // Import class Protobuf

public class AerospikeReadProto {
    public static void main(String[] args) {
        AerospikeClient client = new AerospikeClient("localhost", 3000);

        String namespace = "test";
        String setName = "users";
        Key key = new Key(namespace, setName, "userAlice");

        // 🟢 Đọc dữ liệu từ Aerospike
        Record record = client.get(null, key);

        if (record != null) {
            // 🟢 Chuyển byte[] thành Protobuf Object
            byte[] personBytes = (byte[]) record.getValue("personData");
            try {
                // 🟢 Chuyển byte[] thành đối tượng Protobuf
                Simple.Person person = Simple.Person.parseFrom(personBytes);

                // 🟢 In thông tin ra màn hình
                System.out.println("👤 Name: " + person.getName());
                System.out.println("📅 Age: " + person.getAge());
                System.out.println("📧 Email: " + person.getEmail());
            } catch (InvalidProtocolBufferException e) {
                System.err.println("❌ Lỗi khi parse dữ liệu Protobuf: " + e.getMessage());
            }
        } else {
            System.out.println("❌ Không tìm thấy dữ liệu!");
        }

        client.close();
    }
}
