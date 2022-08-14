package com.java.kafkaStream.chapter4.model.join;

import com.java.kafkaStream.chapter4.model.Player;
import com.java.kafkaStream.chapter4.model.Product;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class PlayerWithProduct {
    private Player player;
    private Product product;
}
