package com.github.fevernova.task.marketdepth.books;


import com.google.common.collect.Maps;


public class BidDepthBooks extends DepthBooks {


    public BidDepthBooks() {

        super(Maps.newTreeMap((l1, l2) -> l2.compareTo(l1)));
    }


    @Override protected long defaultPrice() {

        return 0L;
    }


    @Override protected boolean newEdgePrice(long tmpPrice) {

        return super.cachePrice < tmpPrice;
    }
}
