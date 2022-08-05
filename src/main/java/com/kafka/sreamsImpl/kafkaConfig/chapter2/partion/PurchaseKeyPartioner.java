package com.kafka.sreamsImpl.kafkaConfig.chapter2.partion;

import com.kafka.sreamsImpl.model.PurchaseKey;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import java.nio.charset.StandardCharsets;

public class PurchaseKeyPartioner extends DefaultPartitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String newKey=null;
        if(ObjectUtils.isNotEmpty(key)){
            PurchaseKey purchaseKey = (PurchaseKey) key;
             newKey = purchaseKey.getCustomerId();
            keyBytes = newKey.getBytes(StandardCharsets.UTF_8);

        }
        return this.partition(topic, newKey, keyBytes, value, valueBytes,
                cluster);
    }
}
