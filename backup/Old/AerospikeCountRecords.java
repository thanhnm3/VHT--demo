package com.example;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.Record;
import com.aerospike.client.Key;

public class AerospikeCountRecords {
    public static void main(String[] args) {
        // Kết nối đến Aerospike
        AerospikeClient client = new AerospikeClient("localhost", 3000);

        String namespacePub = "pub";
        String setNamePub = "users";
        String namespaceSub = "sub";
        String setNameSub = "users";

        ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.includeBinData = false; // Chỉ cần đếm số bản ghi, không cần dữ liệu bin

        final int[] countPub = {0}; // Biến đếm số bản ghi trong namespace pub
        final int[] countSub = {0}; // Biến đếm số bản ghi trong namespace sub

        try {
            client.scanAll(scanPolicy, namespacePub, setNamePub, new ScanCallback() {
                @Override
                public void scanCallback(Key key, Record record) throws AerospikeException {
                    countPub[0]++;
                }
            });

            client.scanAll(scanPolicy, namespaceSub, setNameSub, new ScanCallback() {
                @Override
                public void scanCallback(Key key, Record record) throws AerospikeException {
                    countSub[0]++;
                }
            });

            System.out.println("🎯 Tổng số bản ghi trong set 'users' của namespace 'pub': " + countPub[0]);
            System.out.println("🎯 Tổng số bản ghi trong set 'users' của namespace 'sub': " + countSub[0]);
        } catch (AerospikeException e) {
            e.printStackTrace();
        } finally {
            // Đóng kết nối
            client.close();
        }
    }
}
