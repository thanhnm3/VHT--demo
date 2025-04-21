package com.test;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

public class test {
    public static void main(String[] args) {
        // Load config from .env
        io.github.cdimascio.dotenv.Dotenv dotenv = io.github.cdimascio.dotenv.Dotenv.configure().directory("service//.env").load();
        String host1 = dotenv.get("AEROSPIKE_PRODUCER_HOST");
        int port1 = Integer.parseInt(dotenv.get("AEROSPIKE_PRODUCER_PORT"));
        String host2 = dotenv.get("AEROSPIKE_CONSUMER_HOST");
        int port2 = Integer.parseInt(dotenv.get("AEROSPIKE_CONSUMER_PORT"));
        String namespace1 = dotenv.get("PRODUCER_NAMESPACE");
        String namespace2 = dotenv.get("CONSUMER_NAMESPACE");
        String setName = dotenv.get("PRODUCER_SET_NAME");

        AerospikeClient db1 = new AerospikeClient(host1, port1);
        AerospikeClient db2 = new AerospikeClient(host2, port2);
        try {
            // Lấy tất cả key từ db1
            Statement stmt = new Statement();
            stmt.setNamespace(namespace1);
            stmt.setSetName(setName);
            List<String> keyList = new ArrayList<>();
            RecordSet rs = db1.query(null, stmt);
            while (rs.next()) {
                Key key = rs.getKey();
                if (key.userKey != null) {
                    keyList.add(key.userKey.toString());
                }
            }
            rs.close();

            if (keyList.isEmpty()) {
                System.out.println("Không có key nào trong DB1.");
                return;
            }

            // Lấy tối đa 1000 key ngẫu nhiên (hoặc ít hơn nếu không đủ)
            int sampleSize = Math.min(1000, keyList.size());
            Random random = new Random();
            List<String> sampledKeys = new ArrayList<>();
            List<String> tempKeyList = new ArrayList<>(keyList);
            for (int i = 0; i < sampleSize; i++) {
                int idx = random.nextInt(tempKeyList.size());
                sampledKeys.add(tempKeyList.remove(idx));
            }

            int same = 0, diff = 0, missing = 0;
            for (String randomKey : sampledKeys) {
                Key key1 = new Key(namespace1, setName, randomKey);
                Key key2 = new Key(namespace2, setName, randomKey);

                Record rec1 = db1.get(null, key1);
                Record rec2 = db2.get(null, key2);

                Object personData1 = rec1 != null ? rec1.getValue("personData") : null;
                Object personData2 = rec2 != null ? rec2.getValue("personData") : null;

                if (rec1 == null || rec2 == null) {
                    missing++;
                    System.out.println("Key " + randomKey + ": Thieu ban ghi o " + (rec1 == null ? "DB1" : "DB2"));
                } else {
                    boolean isEqual;
                    if (personData1 == null && personData2 == null) {
                        isEqual = true;
                    } else if (personData1 instanceof byte[] && personData2 instanceof byte[]) {
                        isEqual = Arrays.equals((byte[]) personData1, (byte[]) personData2);
                    } else {
                        isEqual = personData1 != null && personData1.equals(personData2);
                    }
                    if (isEqual) {
                        same++;
                    } else {
                        diff++;
                        System.out.println("Key " + randomKey + ": SAI (personData khac nhau)");
                    }
                }
            }
            System.out.println("Tong so key kiem tra: " + sampleSize);
            System.out.println("Giong nhau: " + same);
            System.out.println("Khac nhau: " + diff);
            System.out.println("Thieu ban ghi: " + missing);
        } finally {
            db1.close();
            db2.close();
        }
    }
}
