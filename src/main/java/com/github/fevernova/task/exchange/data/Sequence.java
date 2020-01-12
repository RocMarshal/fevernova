package com.github.fevernova.task.exchange.data;


public class Sequence {


    private long value;


    public void set(long value) {

        this.value = value;
    }


    public long get() {

        return this.value;
    }


    public long getAndIncrement() {

        return this.value++;
    }
}
