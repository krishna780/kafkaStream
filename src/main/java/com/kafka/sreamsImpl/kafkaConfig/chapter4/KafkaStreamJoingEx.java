package com.kafka.sreamsImpl.kafkaConfig.chapter4;

import com.kafka.sreamsImpl.kafkaConfig.model.CorrelatedPurchase;
import com.kafka.sreamsImpl.kafkaConfig.model.Purchase;
import com.kafka.sreamsImpl.kafkaConfig.util.serde.StreamsSerdes;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
@AllArgsConstructor
 class KafkaStreamJoingEx {
    StreamsBuilder streamsBuilder;

    private void implementMethod() throws InterruptedException {
        StreamsConfig streamsConfig=new StreamsConfig(getProperties());
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();
        KeyValueMapper<String, Purchase, KeyValue<String,Purchase>> custIdCCMasking =
                (k, v)-> {
            Purchase masked = Purchase.builder(v).maskCreditCard().build();
            return new KeyValue<>(masked.getCustomerId(),masked);
        };
        Predicate<String, Purchase> predicateCoffee=(key,purchase)->purchase.getDepartment().equals("coffee");
        Predicate<String, Purchase> predicateElectronices=(key,purchase)->purchase.
                getDepartment().equals("Electronices");

        KStream<String, Purchase> purchaseKStream = streamsBuilder.stream("transaction", Consumed.with(stringSerde, purchaseSerde))
                .map(custIdCCMasking);
        KStream<String, Purchase>[] branchStream = purchaseKStream.selectKey((k, v) ->
                        v.getCustomerId()).branch(predicateCoffee,predicateElectronices);
        KStream<String, Purchase> coffeeStream = branchStream[0];
        KStream<String, Purchase> electronicStream = branchStream[2];
        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner=new PurchaseJoiner();

        JoinWindows twentyMinuteWindow =  JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofHours(20L));
        KStream<String, CorrelatedPurchase> joinedKStream = coffeeStream.join(electronicStream,
                purchaseJoiner,
                twentyMinuteWindow);
        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(), streamsConfig);
        kafkaStreams.start();
        Thread.sleep(65000);
        kafkaStreams.close();
    }

     private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        return props;
    }
}
