package com.java.kafkaStream.chapter7.processor;

import com.java.kafkaStream.chapter7.model.DigitalTwin;
import com.java.kafkaStream.chapter7.model.TurbineState;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class DigitalTwinProcessor implements Processor<String, TurbineState,String, DigitalTwin> {

   private ProcessorContext<String,DigitalTwin> context;
   private KeyValueStore<String, DigitalTwin>  keyValueStore;
    @Override
    public void init(ProcessorContext<String, DigitalTwin> context) {
      this.context=context;
      keyValueStore=context.getStateStore("digital-twin-token");
    }

    @Override
    public void process(Record<String, TurbineState> record) {
        String key=record.key();
       TurbineState value=record.value();
        DigitalTwin digitalTwin =  keyValueStore.get(key);
       if(digitalTwin==null){
             digitalTwin=new DigitalTwin();
         }

        if (value.getType() == TurbineState.Type.DESIRED) {
            digitalTwin.setDesired(value);
        } else if (value.getType() == TurbineState.Type.REPORTED) {
            digitalTwin.setReported(value);
        }
        keyValueStore.put(key, digitalTwin);
        Record<String, DigitalTwin> newRecord =
                new Record<>(record.key(), digitalTwin, record.timestamp());
        context.forward(newRecord);

    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
