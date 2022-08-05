package com.kafka.sreamsImpl.kafkaConfig.chapter5;

import com.kafka.sreamsImpl.kafkaConfig.model.StockTickerData;
import com.kafka.sreamsImpl.kafkaConfig.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

import static com.kafka.sreamsImpl.kafkaConfig.clients.producer.MockDataProducer.STOCK_TICKER_TABLE_TOPIC;

public class KStreamVsKTableExample {
    public static void main(String[] args) throws InterruptedException {

         StreamsConfig streamsConfig=new StreamsConfig(getProperties());
        StreamsBuilder streamsBuilder=new StreamsBuilder();
        KStream<String, StockTickerData> kStream = streamsBuilder.stream(STOCK_TICKER_TABLE_TOPIC);
        KTable<String, StockTickerData> kTable = streamsBuilder.table(STOCK_TICKER_TABLE_TOPIC);
        kTable.toStream().print(Printed.<String, StockTickerData>toSysOut().withLabel("withLabel"));
        kStream.print(Printed.<String, StockTickerData>toSysOut().withLabel("withLabel"));
        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(),streamsConfig);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(3000L);
        kafkaStreams.close();
    }
        private static Properties getProperties() {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamVSKTable_app");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "KStreamVSKTable_group");
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KStreamVSKTable_client");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "15000");
            //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
            props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StreamsSerdes.StockTickerSerde().getClass().getName());
            props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
            return props;
        }

}
