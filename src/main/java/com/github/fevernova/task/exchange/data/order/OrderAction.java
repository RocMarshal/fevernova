package com.github.fevernova.task.exchange.data.order;


public enum OrderAction {
    ASK(0),//卖
    BID(1);//买

    public byte code;


    OrderAction(int code) {

        this.code = (byte) code;
    }


    public static OrderAction of(int code) {

        switch (code) {
            case 0:
                return ASK;
            case 1:
                return BID;
            default:
                throw new IllegalArgumentException("unknown OrderAction:" + code);
        }
    }
}
