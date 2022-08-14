package com.java.kafkaStream.chapter7;

import com.java.kafkaStream.chapter7.SerDeserialization.JsonSerdes;
import com.java.kafkaStream.chapter7.model.DigitalTwin;
import com.java.kafkaStream.chapter7.model.TurbineState;
import com.java.kafkaStream.chapter7.processor.DigitalTwinValueTransformerWithKey;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.List;

public class CombinedApp {
    public static Config config= ConfigFactory.load();
    public static void main(String[] args) {
        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String, TurbineState> turbineState = streamsBuilder.stream("Desired-state-event", Consumed.with(Serdes.String(), JsonSerdes.TurbineState()));

        KStream<String, TurbineState> highWinds = streamsBuilder
                .stream("reported-state-events",
                        Consumed.with(Serdes.String(),
                JsonSerdes.TurbineState()))
                .flatMapValues((key, reported)->{
            List<TurbineState> turbineStates=new ArrayList<>();
            turbineStates.add(reported);
            if (reported.getWindSpeedMph()>65
                    && reported.getPower()== TurbineState.Power.ON){
                TurbineState clone = TurbineState.clone(reported);
                clone.setPower(TurbineState.Power.OFF);
                clone.setType(TurbineState.Type.DESIRED);;
                turbineStates.add(clone);
            }
            return turbineStates;
        }).merge(turbineState);


        StoreBuilder<KeyValueStore<String, DigitalTwin>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("digital-twin-store"), Serdes.String(), JsonSerdes.DigitalTwin());

        streamsBuilder.addStateStore(storeBuilder);

        highWinds.transformValues(DigitalTwinValueTransformerWithKey::new, "digital-twin-store")
                .to("digital-twins", Produced.with(Serdes.String(),JsonSerdes.DigitalTwin()));

    }
}
