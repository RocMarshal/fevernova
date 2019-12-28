package com.github.fevernova.io.data.type.impl.date;


import com.github.fevernova.io.data.type.impl.UAbstDate;


public class UTime extends UAbstDate {


    public UTime(boolean lazy) {

        super(lazy);
    }


    @Override
    protected String getFormatString() {

        return "HH:mm:ss";
    }
}
