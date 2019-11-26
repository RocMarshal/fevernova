package com.github.fevernova.data.type.impl.date;


import com.github.fevernova.data.type.impl.UAbstDate;


public class UDate extends UAbstDate {


    public UDate(boolean lazy) {

        super(lazy);
    }


    @Override
    protected String getFormatString() {

        return "yyyy-MM-dd";
    }
}
