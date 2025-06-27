package com.example.pipeline;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.example.pipeline.service.config.Config;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.proto.ProtoSubscriberInfo;
import com.example.pipeline.proto.ProtoSubscriber;
import com.example.pipeline.proto.ProtoBalance;
import com.example.pipeline.proto.ProtoAcmBalance;
import com.example.pipeline.proto.ProtoProduct;
import com.example.pipeline.proto.ProtoCharacteristic;
import com.example.pipeline.proto.ProtoHistory;


import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomInsert {

    private static final String[] REGIONS = {
        "north", "central", "south"
    };

    public static void main(String[] args) {
        try {
            // Load configuration from config.yaml
            Config config = ConfigLoader.getConfig();
            if (config == null) {
                throw new IllegalStateException("Khong the load cau hinh");
            }

            // Lay cau hinh Producer
            Config.Producer producer = config.getProducers().get(0);
            String producerHost = producer.getHost();
            int producerPort = producer.getPort();
            String producerNamespace = producer.getNamespace();
            String producerSetName = producer.getSet();

            System.out.println("=== Bat dau Random Insert ===");
            System.out.println("Producer Host: " + producerHost);
            System.out.println("Producer Port: " + producerPort);
            System.out.println("Producer Namespace: " + producerNamespace);
            System.out.println("Producer Set Name: " + producerSetName);
            System.out.println("===============================");

            // Tạo mảng host cho cả 2 node
            Host[] hostsArr = new Host[] {
                new Host("aerospike", 3000),
                new Host("aerospike-replica", 3000)
            };

            // Tạo duy nhất một instance AerospikeClient
            AerospikeClient client = new AerospikeClient(null, hostsArr);
            System.out.println("Ket noi den Aerospike thanh cong!");

            // Cấu hình WritePolicy tối ưu cho multi-node
            WritePolicy policy = new WritePolicy();
            policy.sendKey = true;
            policy.connectTimeout = 1000; // ms
            policy.socketTimeout = 200;   // ms
            policy.totalTimeout = 2000;   // ms
            policy.maxRetries = 2;
            policy.sleepBetweenRetries = 3000; // ms

            int numThreads = 1;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<?>> futures = new ArrayList<>();

            int maxRecordsPerRegion = 100_000; // So ban ghi moi mien 
            Map<String, AtomicInteger> regionCounters = new ConcurrentHashMap<>();
            for (String region : REGIONS) {
                regionCounters.put(region, new AtomicInteger(0));
            }

            // 🛠 Luồng thực hiện ghi dữ liệu cho từng miền
            for (String region : REGIONS) {
                List<String> provinces = config.getRegion_groups().getProvincesByRegion(region);
                if (provinces == null || provinces.isEmpty()) {
                    System.out.println("Warning: No provinces found for region " + region);
                    continue;
                }

                Future<?> future = executor.submit(() -> {
                    try {
                        AtomicInteger regionCounter = regionCounters.get(region);
                        for (int i = 1; i <= maxRecordsPerRegion; i++) {
                            try {
                                // Generate UUID for user_id
                                String userId = UUID.randomUUID().toString();
                                
                                // Random phone number
                                String phoneNumber = String.format("09%d", ThreadLocalRandom.current().nextInt(10000000, 100000000));
                                
                                
                                // Random province from this region using config
                                String province = provinces.get(ThreadLocalRandom.current().nextInt(provinces.size()));

                                // Create ProtoBalance
                                ProtoBalance balance = ProtoBalance.newBuilder()
                                    .setId(ThreadLocalRandom.current().nextLong())
                                    .setGross(ThreadLocalRandom.current().nextLong())
                                    .setConsume(ThreadLocalRandom.current().nextLong())
                                    .setReserve(ThreadLocalRandom.current().nextLong())
                                    .setBalType(ThreadLocalRandom.current().nextLong())
                                    .setQuotaMax(ThreadLocalRandom.current().nextLong())
                                    .setRecurringDay(ThreadLocalRandom.current().nextLong())
                                    .setOwnerValue("OWNER" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setEffDate(System.currentTimeMillis())
                                    .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                                    .setUpdateDate(System.currentTimeMillis())
                                    .setState(1)
                                    .addAllCharIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .setLevel(ThreadLocalRandom.current().nextInt(5))
                                    .setOf(ThreadLocalRandom.current().nextLong())
                                    .build();

                                // Create ProtoAcmBalance
                                ProtoAcmBalance acmBalance = ProtoAcmBalance.newBuilder()
                                    .setId(ThreadLocalRandom.current().nextLong())
                                    .setValue(ThreadLocalRandom.current().nextLong())
                                    .setReserve(ThreadLocalRandom.current().nextLong())
                                    .setBalType(ThreadLocalRandom.current().nextLong())
                                    .setBillingCycleId(ThreadLocalRandom.current().nextLong())
                                    .setLimit(ThreadLocalRandom.current().nextLong())
                                    .setEffDate(System.currentTimeMillis())
                                    .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                                    .setUpdateDate(System.currentTimeMillis())
                                    .setState(1)
                                    .addAllCharIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .setLevel(ThreadLocalRandom.current().nextInt(5))
                                    .setOf(ThreadLocalRandom.current().nextLong())
                                    .build();

                                // Create ProtoProduct
                                ProtoProduct product = ProtoProduct.newBuilder()
                                    .setId(ThreadLocalRandom.current().nextLong())
                                    .setProductOfferingId(ThreadLocalRandom.current().nextLong())
                                    .addAllMemberList(Arrays.asList("MEMBER1", "MEMBER2"))
                                    .setRecurringDay(ThreadLocalRandom.current().nextLong())
                                    .setEffDate(System.currentTimeMillis())
                                    .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                                    .setUpdateDate(System.currentTimeMillis())
                                    .setState(1)
                                    .addAllCharIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .setLevel(ThreadLocalRandom.current().nextInt(5))
                                    .setOf(ThreadLocalRandom.current().nextLong())
                                    .build();

                                // Create ProtoCharacteristic
                                ProtoCharacteristic characteristic = ProtoCharacteristic.newBuilder()
                                    .setId(ThreadLocalRandom.current().nextLong())
                                    .setCharSpecId(ThreadLocalRandom.current().nextLong())
                                    .setBillingCycleId(ThreadLocalRandom.current().nextLong())
                                    .setValue("VALUE" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setLongValue(ThreadLocalRandom.current().nextLong())
                                    .setEffDate(System.currentTimeMillis())
                                    .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                                    .setUpdateDate(System.currentTimeMillis())
                                    .setState(1)
                                    .addAllCharIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .setLevel(ThreadLocalRandom.current().nextInt(5))
                                    .setOf(ThreadLocalRandom.current().nextLong())
                                    .build();

                                // Create ProtoHistory
                                ProtoHistory history = ProtoHistory.newBuilder()
                                    .setId(ThreadLocalRandom.current().nextLong())
                                    .setType(ThreadLocalRandom.current().nextInt(5))
                                    .setEffDate(System.currentTimeMillis())
                                    .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                                    .setUpdateDate(System.currentTimeMillis())
                                    .setState(1)
                                    .addAllCharIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .setLevel(ThreadLocalRandom.current().nextInt(5))
                                    .setOf(ThreadLocalRandom.current().nextLong())
                                    .setContent("History content " + ThreadLocalRandom.current().nextInt(1000000))
                                    .build();

                                // Create ProtoSubscriber
                                ProtoSubscriber subscriber = ProtoSubscriber.newBuilder()
                                    .setMsisdn(phoneNumber)
                                    .setSubId(ThreadLocalRandom.current().nextLong())
                                    .setCustId(ThreadLocalRandom.current().nextLong())
                                    .setIsDefault(ThreadLocalRandom.current().nextBoolean())
                                    .setSubType(ThreadLocalRandom.current().nextInt(5))
                                    .setStateSet("ACTIVE")
                                    .setPrecharge(ThreadLocalRandom.current().nextLong())
                                    .addAllBalanceIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .addAllAcmBalanceIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .addAllProductIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .addAllGroupIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .addAllMembershipIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .addAllVpnGroupNameList(Arrays.asList("VPN1", "VPN2"))
                                    .putMapSessionTypeLastest(1, "LAST_SESSION")
                                    .setMainProductId(ThreadLocalRandom.current().nextLong())
                                    .addAllCellList(Arrays.asList("CELL1", "CELL2"))
                                    .setLangId(ThreadLocalRandom.current().nextInt(3))
                                    .setRegion(region)
                                    .setLastUpdate(System.currentTimeMillis())
                                    .setImsi("IMSI" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setIccid("ICCID" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setPassword("PASS" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setBccsSubId("BCCS" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setBccsCustId("CUST" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setRegType("PREPAID")
                                    .setSubcategory("INDIVIDUAL")
                                    .setContractId("CONT" + ThreadLocalRandom.current().nextInt(1000000))
                                    .setCustType("RESIDENTIAL")
                                    .setCustVip("NORMAL")
                                    .addAllZoneList(Arrays.asList("ZONE1", "ZONE2"))
                                    .setProvince(province)
                                    .setEmail("user" + ThreadLocalRandom.current().nextInt(1000000) + "@example.com")
                                    .setAddress("Address " + ThreadLocalRandom.current().nextInt(1000))
                                    .setFirstName("First" + ThreadLocalRandom.current().nextInt(1000))
                                    .setLastName("Last" + ThreadLocalRandom.current().nextInt(1000))
                                    .setCompleteDate(System.currentTimeMillis())
                                    .setBirthDay(System.currentTimeMillis() - (ThreadLocalRandom.current().nextInt(365) * 24 * 60 * 60 * 1000L))
                                    .setStartNotifyTime(System.currentTimeMillis())
                                    .setEndNotifyTime(System.currentTimeMillis() + (ThreadLocalRandom.current().nextInt(365) * 24 * 60 * 60 * 1000L))
                                    .setCountTopupTotal(ThreadLocalRandom.current().nextLong())
                                    .setCountTopupFailure(ThreadLocalRandom.current().nextLong())
                                    .setCountTopupSuccess(ThreadLocalRandom.current().nextLong())
                                    .setSex(ThreadLocalRandom.current().nextInt(2))
                                    .setEffDate(System.currentTimeMillis())
                                    .setExpDate(System.currentTimeMillis() + (365 * 24 * 60 * 60 * 1000L))
                                    .setUpdateDate(System.currentTimeMillis())
                                    .setState(1)
                                    .addAllCharIdList(Arrays.asList(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()))
                                    .setLevel(ThreadLocalRandom.current().nextInt(5))
                                    .setOf(ThreadLocalRandom.current().nextLong())
                                    .build();

                                // Create ProtoSubscriberInfo with all related objects
                                ProtoSubscriberInfo subscriberInfo = ProtoSubscriberInfo.newBuilder()
                                    .setSubscriber(subscriber)
                                    .putBalance(ThreadLocalRandom.current().nextLong(), balance)
                                    .putAcmBalance(ThreadLocalRandom.current().nextLong(), acmBalance)
                                    .putProduct(ThreadLocalRandom.current().nextLong(), product)
                                    .putCharacteristic(ThreadLocalRandom.current().nextLong(), characteristic)
                                    .putHistory(ThreadLocalRandom.current().nextLong(), history)
                                    .build();

                                Key key = new Key(producerNamespace, producerSetName, userId.getBytes());
                                
                                // Store each field as a map to reduce bin count
                                List<Bin> bins = new ArrayList<>();
                                // Store subscriber fields
                                Map<String, Object> subscriberData = new HashMap<>();
                                subscriberData.put("m", subscriber.getMsisdn());
                                subscriberData.put("si", subscriber.getSubId());
                                subscriberData.put("ci", subscriber.getCustId());
                                subscriberData.put("df", subscriber.getIsDefault());
                                subscriberData.put("st", subscriber.getSubType());
                                subscriberData.put("ss", subscriber.getStateSet());
                                subscriberData.put("pc", subscriber.getPrecharge());
                                subscriberData.put("bl", subscriber.getBalanceIdListList());
                                subscriberData.put("al", subscriber.getAcmBalanceIdListList());
                                subscriberData.put("pl", subscriber.getProductIdListList());
                                subscriberData.put("gl", subscriber.getGroupIdListList());
                                subscriberData.put("ml", subscriber.getMembershipIdListList());
                                subscriberData.put("vl", subscriber.getVpnGroupNameListList());
                                subscriberData.put("ms", subscriber.getMapSessionTypeLastestMap());
                                subscriberData.put("mp", subscriber.getMainProductId());
                                subscriberData.put("cl", subscriber.getCellListList());
                                subscriberData.put("li", subscriber.getLangId());
                                
                                subscriberData.put("r", region);
                                subscriberData.put("im", subscriber.getImsi());
                                subscriberData.put("ic", subscriber.getIccid());
                                subscriberData.put("pw", subscriber.getPassword());
                                subscriberData.put("bs", subscriber.getBccsSubId());
                                subscriberData.put("bc", subscriber.getBccsCustId());
                                subscriberData.put("rt", subscriber.getRegType());
                                subscriberData.put("sc", subscriber.getSubcategory());
                                subscriberData.put("cid", subscriber.getContractId());
                                subscriberData.put("ct", subscriber.getCustType());
                                subscriberData.put("cv", subscriber.getCustVip());
                                subscriberData.put("zl", subscriber.getZoneListList());
                                subscriberData.put("p", province);
                                
                                bins.add(new Bin("sub", subscriberData));

                                // Tách trường lastUpdate ra thành bin riêng
                                long lastUpdate = System.currentTimeMillis();
                                bins.add(new Bin("lu", lastUpdate));

                                // Thêm bin để kiểm soát kích cỡ dữ liệu (không quan trọng)
                                // Sử dụng byte array để dễ kiểm soát kích cỡ:
                                // - Kích cỡ nhỏ: 10-50 bytes
                                // - Kích cỡ vừa: 50-100 bytes  
                                // - Kích cỡ lớn: 100-200 bytes
                                // - Kích cỡ rất lớn: 200-500 bytes
                                int ctrlSize = ThreadLocalRandom.current().nextInt(50, 100);
                                byte[] randomBytes = new byte[ctrlSize];
                                ThreadLocalRandom.current().nextBytes(randomBytes);
                                bins.add(new Bin("ctrl", randomBytes));

                                // Store balance data
                                Map<String, Map<String, Object>> balanceData = new HashMap<>();
                                for (Map.Entry<Long, ProtoBalance> entry : subscriberInfo.getBalanceMap().entrySet()) {
                                    ProtoBalance bal = entry.getValue();
                                    Map<String, Object> balData = new HashMap<>();
                                    balData.put("i", bal.getId());
                                    balData.put("g", bal.getGross());
                                    balData.put("c", bal.getConsume());
                                    balData.put("r", bal.getReserve());
                                    balData.put("t", bal.getBalType());
                                    balData.put("q", bal.getQuotaMax());
                                    balData.put("d", bal.getRecurringDay());
                                    balData.put("o", bal.getOwnerValue());
                                    balData.put("e", bal.getEffDate());
                                    balData.put("x", bal.getExpDate());
                                    balData.put("u", bal.getUpdateDate());
                                    balData.put("s", bal.getState());
                                    balData.put("l", bal.getCharIdListList());
                                    balData.put("v", bal.getLevel());
                                    balData.put("f", bal.getOf());
                                    balanceData.put("b" + entry.getKey(), balData);
                                }
                                bins.add(new Bin("bal", balanceData));

                                // Store acmBalance data
                                Map<String, Map<String, Object>> acmBalanceData = new HashMap<>();
                                for (Map.Entry<Long, ProtoAcmBalance> entry : subscriberInfo.getAcmBalanceMap().entrySet()) {
                                    ProtoAcmBalance acmBal = entry.getValue();
                                    Map<String, Object> acmBalData = new HashMap<>();
                                    acmBalData.put("i", acmBal.getId());
                                    acmBalData.put("v", acmBal.getValue());
                                    acmBalData.put("r", acmBal.getReserve());
                                    acmBalData.put("t", acmBal.getBalType());
                                    acmBalData.put("b", acmBal.getBillingCycleId());
                                    acmBalData.put("l", acmBal.getLimit());
                                    acmBalData.put("e", acmBal.getEffDate());
                                    acmBalData.put("x", acmBal.getExpDate());
                                    acmBalData.put("u", acmBal.getUpdateDate());
                                    acmBalData.put("s", acmBal.getState());
                                    acmBalData.put("c", acmBal.getCharIdListList());
                                    acmBalData.put("l", acmBal.getLevel());
                                    acmBalData.put("f", acmBal.getOf());
                                    acmBalanceData.put("a" + entry.getKey(), acmBalData);
                                }
                                bins.add(new Bin("acm", acmBalanceData));

                                // Store product data
                                Map<String, Map<String, Object>> productData = new HashMap<>();
                                for (Map.Entry<Long, ProtoProduct> entry : subscriberInfo.getProductMap().entrySet()) {
                                    ProtoProduct prod = entry.getValue();
                                    Map<String, Object> prodData = new HashMap<>();
                                    prodData.put("i", prod.getId());
                                    prodData.put("o", prod.getProductOfferingId());
                                    prodData.put("m", prod.getMemberListList());
                                    prodData.put("d", prod.getRecurringDay());
                                    prodData.put("e", prod.getEffDate());
                                    prodData.put("x", prod.getExpDate());
                                    prodData.put("u", prod.getUpdateDate());
                                    prodData.put("s", prod.getState());
                                    prodData.put("c", prod.getCharIdListList());
                                    prodData.put("l", prod.getLevel());
                                    prodData.put("f", prod.getOf());
                                    productData.put("p" + entry.getKey(), prodData);
                                }
                                bins.add(new Bin("prd", productData));

                                // Store characteristic data
                                Map<String, Map<String, Object>> charData = new HashMap<>();
                                for (Map.Entry<Long, ProtoCharacteristic> entry : subscriberInfo.getCharacteristicMap().entrySet()) {
                                    ProtoCharacteristic charac = entry.getValue();
                                    Map<String, Object> characData = new HashMap<>();
                                    characData.put("i", charac.getId());
                                    characData.put("s", charac.getCharSpecId());
                                    characData.put("b", charac.getBillingCycleId());
                                    characData.put("v", charac.getValue());
                                    characData.put("l", charac.getLongValue());
                                    characData.put("e", charac.getEffDate());
                                    characData.put("x", charac.getExpDate());
                                    characData.put("u", charac.getUpdateDate());
                                    characData.put("s", charac.getState());
                                    characData.put("c", charac.getCharIdListList());
                                    characData.put("l", charac.getLevel());
                                    characData.put("f", charac.getOf());
                                    charData.put("c" + entry.getKey(), characData);
                                }
                                bins.add(new Bin("chr", charData));

                                // Store history data
                                Map<String, Map<String, Object>> historyData = new HashMap<>();
                                for (Map.Entry<Long, ProtoHistory> entry : subscriberInfo.getHistoryMap().entrySet()) {
                                    ProtoHistory hist = entry.getValue();
                                    Map<String, Object> histData = new HashMap<>();
                                    histData.put("i", hist.getId());
                                    histData.put("t", hist.getType());
                                    histData.put("e", hist.getEffDate());
                                    histData.put("x", hist.getExpDate());
                                    histData.put("u", hist.getUpdateDate());
                                    histData.put("s", hist.getState());
                                    histData.put("c", hist.getCharIdListList());
                                    histData.put("l", hist.getLevel());
                                    histData.put("f", hist.getOf());
                                    histData.put("n", hist.getContent());
                                    historyData.put("h" + entry.getKey(), histData);
                                }
                                bins.add(new Bin("his", historyData));

                                // Write all bins to Aerospike
                                client.put(policy, key, bins.toArray(new Bin[0]));


                                // Cập nhật bộ đếm cho region
                                int currentCount = regionCounter.incrementAndGet();

                                // In tiến trình mỗi 100.000 bản ghi
                                if (currentCount % 100_000 == 0) {
                                    System.out.printf("Region %s: Da ghi %d/%d ban ghi%n", 
                                        region, currentCount, maxRecordsPerRegion);
                                }
                            } catch (Exception e) {
                                System.err.printf("Loi khi ghi ban ghi cho region %s: %s%n", region, e.getMessage());
                                e.printStackTrace();
                            }
                        }
                    } catch (Exception e) {
                        System.err.printf("Loi trong thread cho region %s: %s%n", region, e.getMessage());
                        e.printStackTrace();
                    }
                });
                futures.add(future);
            }

            // Đợi tất cả các task hoàn thành
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    System.err.println("Loi khi doi task hoan thanh: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // In tổng số bản ghi cho từng region
            int totalRecords = 0;
            for (String region : REGIONS) {
                int regionCount = regionCounters.get(region).get();
                System.out.printf("Region %s: Tong so ban ghi = %d%n", region, regionCount);
                totalRecords += regionCount;
            }
            System.out.println("\nTong so ban ghi da ghi: " + totalRecords);

            // Đảm bảo đóng client khi xong
            client.close();
        } catch (Exception e) {
            System.err.println("Loi: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
