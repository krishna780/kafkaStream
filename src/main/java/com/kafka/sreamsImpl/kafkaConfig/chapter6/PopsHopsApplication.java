package com.kafka.sreamsImpl.kafkaConfig.chapter6;

import com.kafka.sreamsImpl.kafkaConfig.model.BeerPurchase;
import com.kafka.sreamsImpl.kafkaConfig.util.serializer.JsonDeserializer;
import com.kafka.sreamsImpl.kafkaConfig.util.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.To;

import static com.kafka.sreamsImpl.kafkaConfig.chapter5.PropertiesEx.getProperties;

public class PopsHopsApplication{
    public static void main(String[] args) {
        StreamsConfig streamsConfig=new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> deserializer = stringSerde.deserializer();
        Serializer<String> serializer = stringSerde.serializer();
        JsonDeserializer<BeerPurchase> beerPurchaseJsonDeserializer = new JsonDeserializer<>(BeerPurchase.class);
        JsonSerializer<BeerPurchase> beerPurchaseJsonSerializer=new JsonSerializer<>();

        Topology toplogy = new Topology();

        To domesticSalesSink = To.child("domestic-beer-sales");
        To internationalSalesSink = To.child("international-beer-sales");
        String purchaseSourceNodeName = "beer-purchase-source";
        String purchaseProcessor = "purchase-processor";

        BeerPurchaseProcessor beerPurchaseProcessor=new BeerPurchaseProcessor(domesticSalesSink,internationalSalesSink);

    }
}
