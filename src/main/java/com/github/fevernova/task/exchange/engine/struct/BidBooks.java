package com.github.fevernova.task.exchange.engine.struct;


import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.google.common.collect.Maps;


public final class BidBooks extends Books {


    public BidBooks() {

        super(Maps.newTreeMap((l1, l2) -> 0 - l1.compareTo(l2)), OrderAction.BID);
    }


    @Override protected long defaultPrice() {

        return 0L;
    }


    @Override protected boolean newPrice(long tmpPrice) {

        return tmpPrice > super.price;
    }

}
