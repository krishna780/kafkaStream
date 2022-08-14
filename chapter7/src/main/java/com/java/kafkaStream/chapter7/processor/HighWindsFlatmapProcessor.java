package com.java.kafkaStream.chapter7.processor;

import com.java.kafkaStream.chapter7.model.TurbineState;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class HighWindsFlatmapProcessor implements Processor<String, TurbineState,String,TurbineState> {

private ProcessorContext<String,TurbineState> context;

@Override
    public void init(ProcessorContext<String, TurbineState> context) {
      this.context=context;
    }

    @Override
    public void process(Record<String, TurbineState> record) {
        TurbineState turbineState=record.value();
        context.forward(record);

        if(turbineState.getWindSpeedMph()>65){
            TurbineState clone = TurbineState.clone(turbineState);
            clone.setPower(TurbineState.Power.OFF);
            clone.setType(TurbineState.Type.DESIRED);
            Record<String,TurbineState> record1=new Record<>(record.key(),clone, record.timestamp());
        context.forward(record1);
        }
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
