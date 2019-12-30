package com.github.fevernova.task.exchange.engine.struct;


import com.google.common.collect.Maps;


public final class AskBooks extends Books {


    public AskBooks() {

        super(Maps.newTreeMap(Long::compareTo));
    }


    @Override protected long defaultPrice() {

        return Long.MAX_VALUE;
    }


    @Override protected boolean newPrice(long tmpPrice) {

        return tmpPrice < super.price;
    }

}
