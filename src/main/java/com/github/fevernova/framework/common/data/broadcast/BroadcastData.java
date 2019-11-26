package com.github.fevernova.framework.common.data.broadcast;


import com.github.fevernova.framework.common.data.Data;


public abstract class BroadcastData implements Data {


    public boolean global = false;

    public boolean onlyOnce = false;

    public boolean align = false;


    @Override public byte[] getBytes() {

        return new byte[0];
    }


    @Override public long getTimestamp() {

        return 0;
    }
}
