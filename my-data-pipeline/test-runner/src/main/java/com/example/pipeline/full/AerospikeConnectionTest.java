package com.example.pipeline.full;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.example.pipeline.service.ConfigLoader;
import com.example.pipeline.service.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AerospikeConnectionTest {
    private static final Logger logger = LoggerFactory.getLogger(AerospikeConnectionTest.class);

    public static void main(String[] args) {
        // Các node Aerospike (host, port, namespace, set)
        // Cluster 1: aerospike (aerospike:3000), aerospike-replica (aerospike-replica:3000)
        // Cluster 2: aerospike2 (aerospike2:3000), aerospike2-replica (aerospike2-replica:3000)
        String[] hosts = {"aerospike", "aerospike-replica", "aerospike2", "aerospike2-replica"};
        int[] ports = {3000, 3000, 3000, 3000};
        String[] nodeNames = {"aerospike", "aerospike-replica", "aerospike2", "aerospike2-replica"};

        // Lấy namespace và set từ config
        Config config = ConfigLoader.getConfig();
        String producerNamespace = config.getProducers().get(0).getNamespace();
        String producerSet = config.getProducers().get(0).getSet();
        String consumerNamespace1 = config.getConsumers().get(0).getNamespace();
        String consumerSet1 = config.getConsumers().get(0).getSet();
        String consumerNamespace2 = config.getConsumers().get(1).getNamespace();
        String consumerSet2 = config.getConsumers().get(1).getSet();
        String consumerNamespace3 = config.getConsumers().get(2).getNamespace();
        String consumerSet3 = config.getConsumers().get(2).getSet();

        // Mapping cho từng node
        String[] namespaces = {producerNamespace, consumerNamespace1, consumerNamespace2, consumerNamespace3};
        String[] sets = {producerSet, consumerSet1, consumerSet2, consumerSet3};

        for (int i = 0; i < hosts.length; i++) {
            String host = hosts[i];
            int port = ports[i];
            String namespace = namespaces[i];
            String set = sets[i];
            String nodeName = nodeNames[i];
            logger.info("\n[{}] Connecting to {}:{} (namespace: {}, set: {})", nodeName, host, port, namespace, set);
            try (AerospikeClient client = new AerospikeClient(new ClientPolicy(), host, port)) {
                if (client.isConnected()) {
                    logger.info("[{}] Connection successful!", nodeName);
                    // Thử đọc 1 bản ghi (nếu có key 1)
                    Key key = new Key(namespace, set, 1);
                    Record record = client.get(null, key);
                    if (record != null) {
                        logger.info("[{}] Read record with key=1: {}", nodeName, record.bins);
                    } else {
                        logger.info("[{}] No record found with key=1", nodeName);
                    }
                } else {
                    logger.error("[{}] Connection failed!", nodeName);
                }
            } catch (Exception e) {
                logger.error("[{}] Exception: {}", nodeName, e.getMessage(), e);
            }
        }
    }
} 