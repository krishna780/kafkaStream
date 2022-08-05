package com.kafka.sreamsImpl.kafkaConfig.chapter6;

import com.kafka.sreamsImpl.kafkaConfig.model.BeerPurchase;
import com.kafka.sreamsImpl.kafkaConfig.model.Currency;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;

import java.text.DecimalFormat;

import static com.kafka.sreamsImpl.kafkaConfig.model.Currency.DOLLARS;

public class BeerPurchaseProcessor extends AbstractProcessor<String, BeerPurchase> {
    private To domesticSalesNode;
    private To internationalSalesNode;

    public BeerPurchaseProcessor(To domesticSalesNode, To internationalSalesNode) {
        this.domesticSalesNode = domesticSalesNode;
        this.internationalSalesNode = internationalSalesNode;
    }

    @Override
    public void process(String key, BeerPurchase beerPurchase) {

        Currency transactionCurrency = beerPurchase.getCurrency();
        if (transactionCurrency != DOLLARS) {
            BeerPurchase dollarBeerPurchase;
            BeerPurchase.Builder builder = BeerPurchase.newBuilder(beerPurchase);
            double internationalSaleAmount = beerPurchase.getTotalSale();
            String pattern = "###.##";
            DecimalFormat decimalFormat = new DecimalFormat(pattern);
            builder.currency(DOLLARS);
            builder.totalSale(Double.parseDouble(decimalFormat.format(transactionCurrency.convertToDollars(internationalSaleAmount))));
            dollarBeerPurchase = builder.build();
            context().forward(key, dollarBeerPurchase,internationalSalesNode);
        } else {
            context().forward(key, beerPurchase, domesticSalesNode);
        }
    }
}
