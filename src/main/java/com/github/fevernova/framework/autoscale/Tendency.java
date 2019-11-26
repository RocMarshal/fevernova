package com.github.fevernova.framework.autoscale;


public enum Tendency {

    INCREMENT(1), DECREMENT(-1), STAY(0);

    public final int num;


    Tendency(int num) {

        this.num = num;
    }
}
