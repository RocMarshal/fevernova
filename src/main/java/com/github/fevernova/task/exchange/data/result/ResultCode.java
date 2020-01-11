package com.github.fevernova.task.exchange.data.result;


public enum ResultCode {

    PLACE(100),

    CANCEL(200),
    CANCEL_USER(201),
    CANCEL_IOC(202),
    CANCEL_FOK(203),

    MATCH(300),

    INVALID(-100);

    public int code;


    ResultCode(int code) {

        this.code = code;
    }
}
