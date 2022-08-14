package com.java.kafkaStream.chapter4.model;

import lombok.Builder;
import lombok.Data;

@Data @Builder
public class Product {
    private Long id;
    private String name;
}
