package com.github.fevernova.task.marketdepth.books;


import com.google.common.collect.Maps;


public class AskDepthBooks extends DepthBooks {


    public AskDepthBooks() {

        super(Maps.newTreeMap(Long::compareTo));
    }


    @Override protected long defaultPrice() {

        return Long.MAX_VALUE;
    }


    @Override protected boolean newEdgePrice(long tmpPrice) {

        return super.cachePrice > tmpPrice;
    }
}
