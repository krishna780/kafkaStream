package com.java.kafkaStream.chapter4.model;

import lombok.*;

@Setter
@NoArgsConstructor
@Getter
@AllArgsConstructor
public class ScoreEvent {
    private Long playerId;
    private  Long productId;
    private Double score;
}
