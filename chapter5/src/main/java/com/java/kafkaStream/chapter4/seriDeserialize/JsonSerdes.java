package com.java.kafkaStream.chapter4.seriDeserialize;

import com.java.kafkaStream.chapter4.model.BodyTemp;
import com.java.kafkaStream.chapter4.model.CombinedVitals;
import com.java.kafkaStream.chapter4.model.Pulse;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {
    public static Serde<Pulse> Pulse() {
        JsonSerializer<Pulse> serializer = new JsonSerializer<>();
        JsonDeserializer<Pulse> deserializer = new JsonDeserializer<>(Pulse.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<BodyTemp> BodyTemp() {
        JsonSerializer<BodyTemp> serializer = new JsonSerializer<>();
        JsonDeserializer<BodyTemp> deserializer = new JsonDeserializer<>(BodyTemp.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<CombinedVitals> CombinedVitals() {
        JsonSerializer<CombinedVitals> serializer = new JsonSerializer<>();
        JsonDeserializer<CombinedVitals> deserializer = new JsonDeserializer<>(CombinedVitals.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
