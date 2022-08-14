package com.java.kafkaStream.chapter4.model;

import lombok.Data;

@Data
public class Pulse implements Vital{
    private String timeStamp;

    @Override
    public String getTimestamp() {
        return timeStamp;
    }
}
