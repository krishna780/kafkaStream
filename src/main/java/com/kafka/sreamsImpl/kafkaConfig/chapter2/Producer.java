package com.kafka.sreamsImpl.kafkaConfig.chapter2;

import com.kafka.sreamsImpl.model.PurchaseKey;
import com.kafka.sreamsImpl.kafkaConfig.chapter2.partion.PurchaseKeyPartioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties=new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.Strin\n" +
                "gSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.Strin\n" +
                "gSerializer");
        properties.put("acks",1);
        properties.put("retries",3);
        properties.put("compression.type","snappy");
        properties.put("partitioner.class", PurchaseKeyPartioner.class.getName());

        KafkaProducer<PurchaseKey, String> kafkaProducer = new KafkaProducer<>(properties);
        PurchaseKey key = new PurchaseKey("12334568", new Date());
        ProducerRecord<PurchaseKey, String> record = new ProducerRecord<>("transactions", key, "{\"item\":\"book\",\"price\":10.99}");
        Callback callback=(metadata, exception)->
        {
            if(exception!=null){
                System.out.println(exception);
            }
        };
        Future<RecordMetadata> send = kafkaProducer.send(record, callback);
        RecordMetadata recordMetadata = send.get();

    }
}
