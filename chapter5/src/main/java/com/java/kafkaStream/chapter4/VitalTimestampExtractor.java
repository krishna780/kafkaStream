package com.java.kafkaStream.chapter4;

import com.java.kafkaStream.chapter4.model.Vital;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

public class VitalTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Vital value = (Vital)record.value();

        if(value!= null && value.getTimestamp()!=null) {
            String timestamp = value.getTimestamp();
            return Instant.parse(timestamp).toEpochMilli();
        }

        return partitionTime;
    }
}
