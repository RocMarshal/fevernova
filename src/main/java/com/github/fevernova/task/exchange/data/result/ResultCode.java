package com.github.fevernova.task.exchange.data.result;


public enum ResultCode {

    PLACE(100),

    CANCEL(200),
    CANCEL_USER(201),
    CANCEL_IOC(202),
    CANCEL_FOK(203),
    CANCEL_POSTONLY(204),

    MATCH(300),

    HEARTBEAT(400);

    public int code;


    ResultCode(int code) {

        this.code = code;
    }


    public static ResultCode of(int code) {

        switch (code) {
            case 100:
                return PLACE;
            case 200:
                return CANCEL;
            case 201:
                return CANCEL_USER;
            case 202:
                return CANCEL_IOC;
            case 203:
                return CANCEL_FOK;
            case 204:
                return CANCEL_POSTONLY;
            case 300:
                return MATCH;
            case 400:
                return HEARTBEAT;
            default:
                throw new IllegalArgumentException("unknown ResultCode:" + code);
        }
    }
}
