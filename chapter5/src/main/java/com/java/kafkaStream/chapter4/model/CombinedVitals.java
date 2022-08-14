package com.java.kafkaStream.chapter4.model;

import lombok.*;


@Data
public class CombinedVitals {
    private final int heartRate;
    private final BodyTemp bodyTemp;
}
