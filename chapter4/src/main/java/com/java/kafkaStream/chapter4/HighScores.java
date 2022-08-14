package com.java.kafkaStream.chapter4;

import com.java.kafkaStream.chapter4.model.join.Enriched;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class HighScores {
    private final TreeSet<Enriched> highScores=new TreeSet<>();
    public HighScores add(final Enriched enriched){
        highScores.add(enriched);
        if (highScores.size()>3){
            highScores.remove(highScores.last());
        }
        return this;
    }
    public List<Enriched> toList(){
       List<Enriched> playerScore=new ArrayList<>();
       for (Enriched enriched:highScores){
           playerScore.add(enriched);
       }
       return playerScore;
    }
}
