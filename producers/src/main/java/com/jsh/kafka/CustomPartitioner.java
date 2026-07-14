package com.jsh.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();
    private String specialKeyName;

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int numSpecialPartitions = (int)(numPartitions * 0.5);
        int partitionIndex = 0;

        if (keyBytes == null) {
            //return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidRecordException("Key should not be null");
        }

        // key가 P001인 경우 0~1 파티션, 아닌경우는 2~4 파티션으로
        // 이 설정은 프로듀서에서 설정해준걸 가져온 거임
        // props.setProperty("custom.specialKey", "P001");
        if (((String) key).equals(specialKeyName)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        logger.info("key:{} is sent to partition:{}", key.toString(), partitionIndex);


        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
