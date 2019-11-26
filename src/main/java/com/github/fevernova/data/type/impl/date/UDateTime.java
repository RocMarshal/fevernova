package com.github.fevernova.data.type.impl.date;


import com.github.fevernova.data.type.impl.UAbstDate;


public class UDateTime extends UAbstDate {


    public UDateTime(boolean lazy) {

        super(lazy);
    }


    @Override
    protected String getFormatString() {

        return "yyyy-MM-dd HH:mm:ss";
    }
}
