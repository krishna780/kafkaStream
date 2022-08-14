package com.java.kafkaStream.chapter7;

import com.java.kafkaStream.chapter7.SerDeserialization.JsonSerdes;
import com.java.kafkaStream.chapter7.model.DigitalTwin;
import com.java.kafkaStream.chapter7.processor.DigitalTwinProcessor;
import com.java.kafkaStream.chapter7.processor.HighWindsFlatmapProcessor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import okhttp3.internal.ws.RealWebSocket;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class ProcessorApp {
   private static Config config= ConfigFactory.load().getConfig("streams");

    public static void main(String[] args) {
        Topology topology=getTopology();
        Properties properties=new Properties();
        config.entrySet().forEach(e->properties.setProperty(e.getKey(), config.getString(e.getKey())));

        KafkaStreams streams=new KafkaStreams(topology,properties);
        streams.cleanUp(); streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        String[] endPointParts=config.getString(StreamsConfig.APPLICATION_SERVER_CONFIG).split(":");

        HostInfo hostInfo=new HostInfo(endPointParts[0],Integer.parseInt(endPointParts[1]));
        RestService service=new RestService(hostInfo,streams);
        service.start();

    }

    public  static  Topology getTopology(){
        Topology topology=new Topology();

        topology.addSource("Desired State Events", Serdes.String().deserializer(),
                JsonSerdes.TurbineState().deserializer(), "desired-state-events");

        topology.addSource("Reported State Events", Serdes.String().deserializer(),
                JsonSerdes.TurbineState().deserializer(), "reported-state-events");

        topology.addProcessor("High Winds FlatMap Processor",
                HighWindsFlatmapProcessor::new, "Reported State Events");

        topology.addProcessor("Digital Twin Processor",
                DigitalTwinProcessor::new,
                "High Winds FlatMap Processor",
                "Desired State Events");

        StoreBuilder<KeyValueStore<String, DigitalTwin>> storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("digital-twin-store"),
                Serdes.String(), JsonSerdes.DigitalTwin());

        topology.addStateStore(storeBuilder,"Digital Twin Processor");

        topology.addSink("Digital Twin Sink","Digital-twins",
                Serdes.String().serializer(),JsonSerdes.DigitalTwin().serializer(),
                "Digital Twin Processor");
        return topology;
    }

}
