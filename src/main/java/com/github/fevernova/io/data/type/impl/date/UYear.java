package com.github.fevernova.io.data.type.impl.date;


import com.github.fevernova.io.data.type.impl.UAbstDate;


public class UYear extends UAbstDate {


    public UYear(boolean lazy) {

        super(lazy);
    }


    @Override
    protected String getFormatString() {

        return "yyyy";
    }
}
