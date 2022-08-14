package com.java.kafkaStream.chapter7.processor;

import com.java.kafkaStream.chapter7.model.DigitalTwin;
import com.java.kafkaStream.chapter7.model.TurbineState;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;

public class DigitalTwinValueTransformerWithKey implements ValueTransformerWithKey<String, TurbineState, DigitalTwin> {

    private ProcessorContext context;
    private KeyValueStore<String,DigitalTwin> keyValueStore;

    @Override
    public void init(ProcessorContext context) {
        this.context=context;
        this.keyValueStore= context.getStateStore("digital-twin-store");
        this.context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME,this::enforceTtl);
        this.context.schedule(Duration.ofSeconds(20), PunctuationType.WALL_CLOCK_TIME,(ts)->context.commit());

    }
    private void enforceTtl(long l) {
        KeyValueIterator<String, DigitalTwin> iterator = keyValueStore.all();
      while (iterator.hasNext()){
          KeyValue<String, DigitalTwin> entry = iterator.next();
          TurbineState reported = entry.value.getReported();
          if(reported==null){
              continue;
          }
          Instant instant = Instant.parse(reported.getTimestamp());

          long hours = Duration.between(instant, Instant.now()).toHours();

          if(hours>=24){
              keyValueStore.delete(entry.key);
          }
      }
      iterator.close();
    }

    @Override
    public DigitalTwin transform(String readOnlyKey, TurbineState value) {
        DigitalTwin digitalTwin = keyValueStore.get(readOnlyKey);
        if(digitalTwin==null){
            digitalTwin=new DigitalTwin();
        }
        if(value.getType()== TurbineState.Type.DESIRED){
            digitalTwin.setDesired(value);
        } else if (value.getType()== TurbineState.Type.REPORTED) {
            digitalTwin.setReported(value);
        }
         keyValueStore.put(readOnlyKey,digitalTwin);
        return digitalTwin;
    }

    @Override
    public void close() {

    }
}
