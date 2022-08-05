package com.kafka.sreamsImpl.kafkaConfig.chapter4;

import com.kafka.sreamsImpl.kafkaConfig.model.CorrelatedPurchase;
import com.kafka.sreamsImpl.kafkaConfig.model.Purchase;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Date;
import java.util.List;

public class PurchaseJoiner implements ValueJoiner<Purchase,Purchase, CorrelatedPurchase> {
    @Override
    public CorrelatedPurchase apply(Purchase purchase, Purchase otherPurchase) {
        CorrelatedPurchase.Builder correlatedPurchase= CorrelatedPurchase.newBuilder();

        Date purchaseDate = purchase.getPurchaseDate();
        double price = purchase.getPrice();
        String itemPurchased = purchase.getItemPurchased();
        String customerId = purchase.getCustomerId();

        Date otherPurchaseDate = otherPurchase.getPurchaseDate();
        double otherPurchasePrice = otherPurchase.getPrice();
        String otherPurchaseItemPurchased = otherPurchase.getItemPurchased();
        String otherPurchaseCustomerId = otherPurchase.getCustomerId();
        return correlatedPurchase
                .withItemsPurchased(List.of(itemPurchased,otherPurchaseItemPurchased))
                .withFirstPurchaseDate(purchaseDate)
                .withSecondPurchaseDate(otherPurchaseDate)
                .withTotalAmount(price+otherPurchasePrice)
                .withCustomerId(customerId.isBlank()?otherPurchaseCustomerId:customerId)
                .build();
    }
}
