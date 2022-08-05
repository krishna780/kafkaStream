package com.kafka.sreamsImpl.kafkaConfig.chapter3;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Locale;
import java.util.Properties;

@Log4j2
public class KafkaStreamsYellingApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.AT_LEAST_ONCE, "processing.guarantee");
        StreamsConfig streamsConfig=new StreamsConfig(props);
        Serde<String> serde = Serdes.String();
       StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> stream = streamsBuilder.stream("new topic", Consumed.with(serde, serde));
       KStream<String, String> toUpperCase=stream.mapValues(s->s.toLowerCase());
       toUpperCase.to("out Topic", Produced.with(serde,serde));
        try (KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig)) {
            kafkaStreams.start();
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
