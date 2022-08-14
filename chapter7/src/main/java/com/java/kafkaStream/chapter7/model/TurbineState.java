package com.java.kafkaStream.chapter7.model;

import lombok.Data;

@Data
public class TurbineState {
    private String timestamp;
    private Double windSpeedMph;

    public enum Power { ON, OFF }
    public enum Type { DESIRED, REPORTED }
    private Power power;
    private Type type;

    public TurbineState(String timestamp, Double windSpeedMph, Power power, Type type) {
        this.timestamp = timestamp;
        this.windSpeedMph = windSpeedMph;
        this.power = power;
        this.type = type;
    }

    public static TurbineState clone(TurbineState reported) {
     return new TurbineState(reported.getTimestamp(),
                reported.getWindSpeedMph(),reported.getPower(),reported.getType());
    }
}
