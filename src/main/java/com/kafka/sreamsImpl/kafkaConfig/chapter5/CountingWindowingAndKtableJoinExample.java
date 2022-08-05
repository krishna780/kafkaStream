package com.kafka.sreamsImpl.kafkaConfig.chapter5;

import com.kafka.sreamsImpl.kafkaConfig.model.StockTransaction;
import com.kafka.sreamsImpl.kafkaConfig.model.TransactionSummary;
import com.kafka.sreamsImpl.kafkaConfig.util.serde.StreamsSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.eclipse.jetty.util.log.Log;

import java.time.Duration;

import static com.kafka.sreamsImpl.kafkaConfig.clients.producer.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class CountingWindowingAndKtableJoinExample {
    public static void main(String[] args) {
        StreamsConfig streamsConfig=new StreamsConfig(PropertiesEx.getProperties());
        StreamsBuilder streamsBuilder=new StreamsBuilder();
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> transactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<TransactionSummary> transactionKeySerde = StreamsSerdes.TransactionSummarySerde();
        KTable<Windowed<TransactionSummary>, Long> longKTable = streamsBuilder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, transactionSerde)
                        .withOffsetResetPolicy(EARLIEST))
                .groupBy((k, v) -> TransactionSummary.from(v))
                .windowedBy(
                        SessionWindows.ofInactivityGapAndGrace
                                (Duration.ofSeconds(20L), Duration.ofMinutes(10L))).count();

        KStream<String, TransactionSummary> map = longKTable.toStream().map((window, count) -> {
            TransactionSummary transactionSummary = window.key();
            String industry = transactionSummary.getIndustry();
            transactionSummary.setSummaryCount(count);
            return KeyValue.pair(industry, transactionSummary);
        });
        KTable<String, String> financialNews = streamsBuilder.table( "financial-news", Consumed.with(EARLIEST));

        ValueJoiner<TransactionSummary, String, String> valueJoiner = (txnct, news) ->
                String.format("%d shares purchased %s related news [%s]", txnct.getSummaryCount(), txnct.getStockTicker(), news);

        KStream<String, String> stringStringKStream = map.leftJoin(financialNews, valueJoiner, Joined.with(stringSerde, transactionKeySerde, stringSerde));
        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(),streamsConfig);
        kafkaStreams.cleanUp();
        kafkaStreams.setUncaughtExceptionHandler((StreamsUncaughtExceptionHandler) new MyExceptionHandler());
    }
}
