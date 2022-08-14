package com.java.kafkaStream.chapter7;

import com.java.kafkaStream.chapter7.model.DigitalTwin;
import io.javalin.Javalin;
import io.javalin.http.Context;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;


@AllArgsConstructor
public class RestService {
    private HostInfo hostInfo;
    private KafkaStreams streams;

    ReadOnlyKeyValueStore<String, DigitalTwin> getStore() {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        "digital-twin-store",
                        QueryableStoreTypes.keyValueStore()));
    }
    void start() {
        Javalin app = Javalin.create().start(hostInfo.port());
        app.get("/devices/:id", this::getDevice);
    }
    void getDevice(Context ctx) {
        String deviceId = ctx.pathParam("id");
        DigitalTwin latestState = getStore().get(deviceId);
        ctx.json(latestState);
    }
}
