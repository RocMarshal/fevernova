package com.github.fevernova.task.exchange.engine.struct;


import com.google.common.collect.Maps;


public class UpConditionBooks extends ConditionBooks {


    public UpConditionBooks() {

        super(Maps.newTreeMap(Long::compareTo));
    }


    @Override protected long defaultPrice() {

        return Long.MAX_VALUE;
    }


    @Override public boolean newEdgePrice(long tmpPrice) {

        return super.price > tmpPrice;
    }
}
