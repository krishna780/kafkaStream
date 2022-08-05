package com.kafka.sreamsImpl.kafkaConfig.chapter5;

import com.kafka.sreamsImpl.kafkaConfig.model.StockTransaction;
import com.kafka.sreamsImpl.kafkaConfig.model.TransactionSummary;
import com.kafka.sreamsImpl.kafkaConfig.util.serde.StreamsSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

import static com.kafka.sreamsImpl.kafkaConfig.chapter5.PropertiesEx.getProperties;
import static com.kafka.sreamsImpl.kafkaConfig.clients.producer.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;

public class GlobalKTableExample {
    public static void main(String[] args) throws InterruptedException {
        StreamsConfig streamsConfig=new StreamsConfig(getProperties());

        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionSummarySerde = StreamsSerdes.TransactionSummarySerde();


        StreamsBuilder builder = new StreamsBuilder();
        long twentySeconds = 1000 * 20;

        KeyValueMapper<Windowed<TransactionSummary>, Long, KeyValue<String, TransactionSummary>> transactionMapper=(k,v)->{
            TransactionSummary transactionSummary = k.key();
            transactionSummary.setSummaryCount(v);
            return KeyValue.pair(transactionSummary.getCustomerId(),transactionSummary);
        };

        KStream<String, TransactionSummary> mapCount = builder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, transactionSerde)
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .groupBy((noKey, trans) -> TransactionSummary.from(trans))
                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ZERO, Duration.ofMinutes(12L)))
                .count().toStream().map(transactionMapper);
        GlobalKTable<String, String> companies_name = builder.globalTable("companies name");
        GlobalKTable<String, String> clients_name = builder.globalTable("client name");
        mapCount.leftJoin(companies_name, (k,v)->v.getStockTicker(), TransactionSummary::withCompanyName)
                .leftJoin(clients_name, (k,v)->v.getCustomerId(), TransactionSummary::withCustomerName);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(1000L);
        kafkaStreams.close();

    }
}
