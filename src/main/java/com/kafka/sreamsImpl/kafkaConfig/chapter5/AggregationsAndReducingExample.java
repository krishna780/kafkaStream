package com.kafka.sreamsImpl.kafkaConfig.chapter5;

import com.kafka.sreamsImpl.kafkaConfig.collectors.FixedSizePriorityQueue;
import com.kafka.sreamsImpl.kafkaConfig.model.ShareVolume;
import com.kafka.sreamsImpl.kafkaConfig.model.StockTransaction;
import com.kafka.sreamsImpl.kafkaConfig.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import static com.kafka.sreamsImpl.kafkaConfig.clients.producer.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;

public class AggregationsAndReducingExample {
    public static void main(String[] args) {
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.ShareVolumeSerde();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.FixedSizePriorityQueueSerde();
        NumberFormat numberFormat = NumberFormat.getInstance();

        Comparator<ShareVolume> shareVolumeComp=(s1,s2)->s2.getShares()-s1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedSizePriorityQueue=new FixedSizePriorityQueue<>(shareVolumeComp,5);

        ValueMapper<FixedSizePriorityQueue, String> valueMapper=arg->{
            StringBuilder stringBuilder=new StringBuilder();
            Iterator<ShareVolume> iterator = arg.iterator();
            int counter=1;
            while (iterator.hasNext()){
                ShareVolume stockVolume = iterator.next();
                stringBuilder.append(counter++).append(")")
                        .append(stockVolume.getSymbol())
                        .append(":")
                        .append(numberFormat.format(stockVolume.getShares())).append(" ");

            }
            return stringBuilder.toString();
        };


    StreamsConfig streamsConfig=new StreamsConfig(getProperties());
    StreamsBuilder streamsBuilder=new StreamsBuilder();
        KTable<String, ShareVolume> shareVolumeKTable = streamsBuilder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde,
                        stockTransactionSerde).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
                .mapValues(st -> ShareVolume.newBuilder(st).build())
                .groupBy((k, v) -> v.getSymbol())
                .reduce(ShareVolume::sum);
       shareVolumeKTable.groupBy((k,v)->KeyValue.pair(v.getIndustry(),v))
             .aggregate(()->fixedSizePriorityQueue,
                     (k,v,agg)->agg.add(v),
                     (k,v,agg)->agg.remove(v))
             .mapValues(valueMapper)
             .toStream() .to("stock-volume-by-company", Produced.with(stringSerde, stringSerde));
    }
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KTable-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KTable-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }

}
