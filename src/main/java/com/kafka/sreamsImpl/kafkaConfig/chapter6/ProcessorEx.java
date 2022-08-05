package com.kafka.sreamsImpl.kafkaConfig.chapter6;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class ProcessorEx implements Processor<Void, String, Void, Void> {
    @Override
    public void init(ProcessorContext<Void, Void> context) {
        Processor.super.init(context);
    }

    @Override
    public void process(Record<Void, String> record) {
        String value = record.value();
        System.out.println(value);
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
