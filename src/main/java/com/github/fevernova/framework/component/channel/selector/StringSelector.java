package com.github.fevernova.framework.component.channel.selector;


import com.google.common.hash.Hashing;


public class StringSelector implements ISelector<String> {


    @Override
    public int getIntVal(String val) {

        return val == null ? 0 : Hashing.murmur3_128().hashBytes(val.getBytes()).asInt();
    }
}
