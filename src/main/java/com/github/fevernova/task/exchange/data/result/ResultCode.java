package com.github.fevernova.task.exchange.data.result;


public enum ResultCode {

    PLACE(100),

    CANCEL(200),
    CANCEL_USER(201),
    CANCEL_IOC(202),
    CANCEL_FOK(203),

    MATCH(300),

    MISS_ORDER(400),
    MISS_ORDER_SUCCESS(401),
    MISS_ORDER_FAILED(402),

    INVALID(-100),
    INVALID_PLACE_DUPLICATE_ORDER_ID(-101),
    INVALID_CANCEL_NO_ORDER_ID(-102);

    private int code;


    ResultCode(int code) {

        this.code = code;
    }
}
