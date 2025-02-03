package dev.gohard.learning_kafka;

import lombok.Getter;

@Getter
public class Customor {
    private int id;
    private String name;

    public Customor(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
