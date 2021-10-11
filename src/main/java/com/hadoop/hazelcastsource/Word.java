package com.hadoop.hazelcastsource;


import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class Word implements Serializable {
    @JsonProperty("active")
    private boolean active;

    @JsonProperty("word")
    String word;

    public Word() {
    }

    public Word(boolean active, String word) {
        this.active = active;
        this.word = word;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}

