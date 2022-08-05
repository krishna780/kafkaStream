package com.kafka.sreamsImpl.kafkaConfig.chapter4;

import com.kafka.sreamsImpl.kafkaConfig.model.Purchase;
import com.kafka.sreamsImpl.kafkaConfig.model.PurchasePattern;
import com.kafka.sreamsImpl.kafkaConfig.model.RewardAccumulator;
import com.kafka.sreamsImpl.kafkaConfig.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class ZMartKafkaStreamsAddStateApp {
    public static void main(String[] args) {
       StreamsConfig streamsConfig=new StreamsConfig(getProperties());
        StreamsBuilder builder=new StreamsBuilder();
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();


        KStream<String,Purchase> purchaseKStream = builder.stream( "transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));
        RewardsStreamPartitioner rewardsStreamPartitioner=new RewardsStreamPartitioner();
        KeyValueBytesStoreSupplier rewardsPointsStore = Stores.inMemoryKeyValueStore("rewardsPointsStore");
        StoreBuilder<KeyValueStore<String, Integer>> keyValueStoreStoreBuilder = Stores.keyValueStoreBuilder(rewardsPointsStore, Serdes.String(), Serdes.Integer());
        builder.addStateStore(keyValueStoreStoreBuilder);
        purchaseKStream.to("customer_transaction", Produced.with(stringSerde, purchaseSerde, rewardsStreamPartitioner));

    }
        private static Properties getProperties() {
            Properties props = new Properties();
            props.put(StreamsConfig.CLIENT_ID_CONFIG, "AddingStateConsumer");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "AddingStateGroupId");
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AddingStateAppId");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
            return props;

    }
}
