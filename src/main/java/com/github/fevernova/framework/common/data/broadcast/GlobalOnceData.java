package com.github.fevernova.framework.common.data.broadcast;


public class GlobalOnceData extends BroadcastData {


    public GlobalOnceData() {

        super.align = false;
        super.global = true;
        super.onlyOnce = true;
    }
}
