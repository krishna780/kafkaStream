package com.java.kafkaStream.chapter4.model.join;

import com.java.kafkaStream.chapter4.model.Product;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Enriched implements Comparable<Enriched>{
    private Long playerId;
    private Long productId;
    private String playerName;
    private String gameName;
    private Double score;

    public Enriched(ScoreWithPlayer scoreWithPlayer, Product product) {
        this.playerId = scoreWithPlayer.getPlayer().getId();
        this.productId = product.getId();
        this.playerName = scoreWithPlayer.getPlayer().getName();
        this.gameName = product.getName();
        this.score = scoreWithPlayer.getScoreEvent().getScore();
    }

    @Override
    public int compareTo(Enriched o) {
        return Double.compare(o.score,score);
    }
}
