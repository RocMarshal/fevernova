package com.github.fevernova.task.exchange.data.order;


public enum OrderType {
    GTC(0),//限价
    IOC(1);//市价

    public byte code;


    OrderType(int code) {

        this.code = (byte) code;
    }


    public static OrderType of(int code) {

        switch (code) {
            case 0:
                return GTC;
            case 1:
                return IOC;
            default:
                throw new IllegalArgumentException("unknown OrderType:" + code);
        }
    }

}
