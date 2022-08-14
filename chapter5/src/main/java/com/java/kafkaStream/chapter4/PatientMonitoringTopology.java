package com.java.kafkaStream.chapter4;

import com.java.kafkaStream.chapter4.model.BodyTemp;
import com.java.kafkaStream.chapter4.model.CombinedVitals;
import com.java.kafkaStream.chapter4.model.Pulse;
import com.java.kafkaStream.chapter4.seriDeserialize.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class PatientMonitoringTopology  {
    public Topology monitoringTopology(){
        StreamsBuilder builder=new StreamsBuilder();
        Consumed<String, Pulse> pulseConsumed = Consumed.with(Serdes.String(), JsonSerdes.Pulse()).withTimestampExtractor(new VitalTimestampExtractor());
        KStream<String, Pulse> pulseKStream = builder.stream("pulse-topic", pulseConsumed);

        Consumed<String, BodyTemp> bodyTempConsumedConsumed = Consumed.with(Serdes.String(), JsonSerdes.BodyTemp()).withTimestampExtractor(new VitalTimestampExtractor());
        KStream<String, BodyTemp> bodyTemp = builder.stream("bodyTemp-topic", bodyTempConsumedConsumed);

        TimeWindows windows = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(4));
        SessionWindows sessionWindows = SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(5));

        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5));
        KTable<Windowed<String>, Long> countPulse = pulseKStream.groupByKey().windowedBy(timeWindows)
                .count(Materialized.as("pulse-count")).suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));


        KStream<String, Long> highPulse = countPulse.toStream().filter((k, v) -> v > 100)
                .map((k, v) -> KeyValue.pair(k.key(), v));

        StreamJoined<String, Long, BodyTemp> joinParams = StreamJoined.with(Serdes.String(), Serdes.Long(), JsonSerdes.BodyTemp());


        ValueJoiner<Long, BodyTemp, CombinedVitals> valueJoiner =(k,v)->
                new CombinedVitals(k.intValue(),v);
        JoinWindows joinWindows =
                JoinWindows
                        .of(Duration.ofSeconds(60))
                        .grace(Duration.ofSeconds(10));

        KStream<String, BodyTemp> highTemp = bodyTemp.filter((k, v) -> v != null && v.getTemperature() != null && v.getTemperature() > 100.4);

        KStream<String, CombinedVitals> vitalsJoined = highPulse.join(highTemp, valueJoiner, joinWindows, joinParams);

        return builder.build();

    }
}
