package com.github.fevernova.data.type.impl.date;


import com.github.fevernova.data.type.impl.UAbstDate;


public class UYear extends UAbstDate {


    public UYear(boolean lazy) {

        super(lazy);
    }


    @Override
    protected String getFormatString() {

        return "yyyy";
    }
}
