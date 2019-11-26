package com.github.fevernova.framework.common.data;


public interface Data {


    default void clearData() {

    }

    byte[] getBytes();

    long getTimestamp();
}
