package com.github.fevernova.framework.component;


public interface DataProvider<K, V> {


    V feedOne(K key);

    void push();
}
