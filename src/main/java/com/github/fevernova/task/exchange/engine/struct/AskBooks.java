package com.github.fevernova.task.exchange.engine.struct;


import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.google.common.collect.Maps;


public final class AskBooks extends Books {


    public AskBooks() {

        super(Maps.newTreeMap(Long::compareTo), OrderAction.ASK);
    }


    @Override protected long defaultPrice() {

        return Long.MAX_VALUE;
    }


    @Override protected boolean newPrice(long tmpPrice) {

        return tmpPrice < super.price;
    }

}
