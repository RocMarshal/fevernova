package com.github.fevernova.task.exchange.data.order;


public enum OrderMode {
    SIMPLE(0),//
    CONDITION(10),//条件单
    CONDITION_UP(11),
    CONDITION_DOWN(12);

    public byte code;


    OrderMode(int code) {

        this.code = (byte) code;
    }


    public static OrderMode of(int code) {

        switch (code) {
            case 0:
                return SIMPLE;
            case 10:
                return CONDITION;
            case 11:
                return CONDITION_UP;
            case 12:
                return CONDITION_DOWN;
            default:
                throw new IllegalArgumentException("Unknown OrderMode:" + code);
        }
    }

}
