package com.java.kafkaStream.chapter4.model.join;

import com.java.kafkaStream.chapter4.model.Player;
import com.java.kafkaStream.chapter4.model.ScoreEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class ScoreWithPlayer {
    private ScoreEvent scoreEvent;
    private Player player;
}
