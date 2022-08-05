package com.kafka.sreamsImpl.kafkaConfig.chapter4;

import com.kafka.sreamsImpl.kafkaConfig.model.Purchase;
import com.kafka.sreamsImpl.kafkaConfig.model.RewardAccumulator;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
@RequiredArgsConstructor
public class PurchaseRewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {

    private KeyValueStore<String, Integer> stateStore;
    private final String storeName;
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext context) {
        this.context=context;
        stateStore = this.context.getStateStore(storeName);
    }

    @Override
    public RewardAccumulator transform(Purchase value) {
        RewardAccumulator rewardAccumulator = RewardAccumulator.builder(value).build();
        Integer integer = stateStore.get(rewardAccumulator.getCustomerId());
        rewardAccumulator.addRewardPoints(integer);
        stateStore.put(rewardAccumulator.getCustomerId(), rewardAccumulator.getTotalRewardPoints());
        return rewardAccumulator;
    }

    @Override
    public void close() {

    }
}
