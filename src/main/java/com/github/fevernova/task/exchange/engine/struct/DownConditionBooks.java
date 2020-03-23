package com.github.fevernova.task.exchange.engine.struct;


import com.google.common.collect.Maps;


public class DownConditionBooks extends ConditionBooks {


    public DownConditionBooks() {

        super(Maps.newTreeMap((l1, l2) -> l2.compareTo(l1)));
    }


    @Override protected long defaultPrice() {

        return 0L;
    }


    @Override public boolean newEdgePrice(long tmpPrice) {

        return super.price < tmpPrice;
    }
}
