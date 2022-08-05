package com.kafka.sreamsImpl.kafkaConfig.chapter3;

import com.kafka.sreamsImpl.kafkaConfig.clients.producer.MockDataProducer;
import com.kafka.sreamsImpl.kafkaConfig.model.Purchase;
import com.kafka.sreamsImpl.kafkaConfig.model.PurchasePattern;
import com.kafka.sreamsImpl.kafkaConfig.model.RewardAccumulator;
import com.kafka.sreamsImpl.kafkaConfig.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

public class KafkaStreamApp {
    public static void main(String[] args) throws InterruptedException {
        StreamsConfig streamsConfig=new StreamsConfig(getProperties());
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> serde = Serdes.String();
        StreamsBuilder streamsBuilder=new StreamsBuilder();
        KStream<String, Purchase> kStream = streamsBuilder.stream("transaction", Consumed.with(serde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());
        KStream<String, PurchasePattern> patternKStream = kStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());
        patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel("pattern"));
        patternKStream.to("pattern", Produced.with(serde,purchasePatternSerde));
        KStream<String, RewardAccumulator> rewardAccumulatorKStream = kStream.mapValues(purchas -> RewardAccumulator.builder(purchas).build());
        rewardAccumulatorKStream.to("reward",Produced.with(serde,rewardAccumulatorSerde));
        MockDataProducer.producePurchaseData();
        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(),streamsConfig);
        kafkaStreams.start();
        Thread.sleep(3000);
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
