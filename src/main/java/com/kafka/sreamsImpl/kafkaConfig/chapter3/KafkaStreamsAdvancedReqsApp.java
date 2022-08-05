package com.kafka.sreamsImpl.kafkaConfig.chapter3;

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
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

public class KafkaStreamsAdvancedReqsApp {
    public static void main(String[] args) {
       StreamsConfig streamsConfig=new StreamsConfig(getProperties());
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();
        StreamsBuilder builder=new StreamsBuilder();
        KStream<String, Purchase> purchasePatternKStream = builder.stream("transaction", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(purchase -> Purchase.builder(purchase).maskCreditCard().build());
        KStream<String, PurchasePattern> purchasePattern = purchasePatternKStream.mapValues(p -> PurchasePattern.builder(p).build());
        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime();
        KStream<Long, Purchase> kStream = purchasePatternKStream.filter((k, v) -> v.getPrice() > 5).selectKey(purchaseDateAsKey);
        kStream.to("purchases", Produced.with(Serdes.Long(),purchaseSerde));
    }
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Kafka-Streams-Job");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streams-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-streams-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
