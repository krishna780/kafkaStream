package com.java.kafkaStream.chapter4;

import com.java.kafkaStream.chapter4.model.Player;
import com.java.kafkaStream.chapter4.model.Product;
import com.java.kafkaStream.chapter4.model.ScoreEvent;
import com.java.kafkaStream.chapter4.model.join.Enriched;
import com.java.kafkaStream.chapter4.model.join.PlayerWithProduct;
import com.java.kafkaStream.chapter4.model.join.ScoreWithPlayer;
import com.java.kafkaStream.chapter4.serialization.JsonSerdes;
import io.javalin.apibuilder.EndpointGroup;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.function.BiFunction;

@Log4j2
public class LeaderboardTopology {
    public static Topology build(){

        StreamsBuilder builder=new StreamsBuilder();
        KStream<String, ScoreEvent> scoreEvent=builder
                .stream("score-events", Consumed.with(Serdes.ByteArray(),
                        JsonSerdes.ScoreEvent()))
                .selectKey((k,v)->v.getPlayerId().toString());

        // create the sharded players table
        KTable<String, Player> players =
                builder.table("players", Consumed.with(Serdes.String(),
                        JsonSerdes.Player()));

        // create the global product table
        GlobalKTable<String, Product> products =
                builder.globalTable("products", Consumed.with(Serdes.String(),
                        JsonSerdes.Product()));

        ValueJoiner<ScoreEvent,Player, ScoreWithPlayer> scoreWithPlayer= ScoreWithPlayer::new;

        ValueJoiner<Player, Product, PlayerWithProduct> endpointGroup = PlayerWithProduct::new;

        Joined<String, ScoreEvent, Player> playerJoinParams = Joined.with(Serdes.String(),
                JsonSerdes.ScoreEvent(), JsonSerdes.Player());

        Joined<String, Player,Product> playerProductJoined=Joined.with(Serdes.String(),
                JsonSerdes.Player(),JsonSerdes.Product());


        KStream<String, ScoreWithPlayer> withPlayers = scoreEvent.join(players,
                     scoreWithPlayer, playerJoinParams);



        // ktable and globalktable join

        KeyValueMapper<String,ScoreWithPlayer,String> keyMapper=(left,score)-> String.valueOf(score.getScoreEvent().getProductId());

        ValueJoiner<ScoreWithPlayer, Product, Enriched> productJoiner= Enriched::new;


        KStream<String, Enriched> withProducts = withPlayers.join(products, keyMapper, productJoiner);

        withProducts.print(Printed.<String, Enriched>toSysOut().withLabel("with-products"));


     
     //groupby¬

        KGroupedStream<String, Enriched> grouped = withProducts.
                groupBy((k, v) -> v.getProductId().toString(),
                Grouped.with(Serdes.String(), JsonSerdes.Enriched()));

        log.info(grouped);
        KGroupedStream<String, Enriched> groupByKey =
                withProducts.groupByKey(Grouped.with(Serdes.String(),
                        JsonSerdes.Enriched()));

        Initializer<HighScores> highScoresInitializer= HighScores::new;

        Aggregator<String, Enriched,HighScores> highScoresAdder=(k,v,a)->a.add(v);

        KTable<String, HighScores> highScore= grouped.
                aggregate(highScoresInitializer, highScoresAdder);

        log.info(highScore);


        KTable<String, HighScores> aggregate = grouped.aggregate
                (highScoresInitializer, highScoresAdder,
                Materialized.<String, HighScores, KeyValueStore<Bytes, byte[]>>as("" +
                                "leader-board").withKeySerde(Serdes.String())
                        .withValueSerde(JsonSerdes.HighScores()));
        aggregate.toStream().to("high-score");


        return builder.build();

    }
}
