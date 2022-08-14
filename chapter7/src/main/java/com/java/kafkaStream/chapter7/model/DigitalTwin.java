package com.java.kafkaStream.chapter7.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DigitalTwin {
    private TurbineState desired;
    private TurbineState reported;
}
