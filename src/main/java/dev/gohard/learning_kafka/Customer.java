package dev.gohard.learning_kafka;

import lombok.Getter;

@Getter
public class Customer {
    private int id;
    private String name;

    public Customer(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
