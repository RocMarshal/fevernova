package com.github.fevernova.framework.component.channel.selector;


import com.google.common.hash.Hashing;


public class BytesSelector implements ISelector<byte[]> {


    @Override public int getIntVal(byte[] val) {

        return val == null ? 0 : Hashing.murmur3_128().hashBytes(val).asInt();
    }
}
