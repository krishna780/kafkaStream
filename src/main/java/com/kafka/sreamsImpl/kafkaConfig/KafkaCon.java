package com.kafka.sreamsImpl.kafkaConfig;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaCon {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"application id");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        StreamsConfig streamsConfig=new StreamsConfig(properties);
        StreamsBuilder builder=new StreamsBuilder();
        Serde<String> string = Serdes.String();
        KStream<String, String> stream = builder.stream("src=topic", Consumed.with(string, string));
        stream.to("out-topic", Produced.with(string,string));
    }
}
