package com.github.fevernova.framework.common.structure.rb;


import java.util.Optional;


public class NonRingBuffer<E> implements IRingBuffer<E> {


    @Override
    public boolean add(E e, long size) {

        return true;
    }


    @Override
    public Optional<E> get() {

        return null;
    }


    @Override
    public int curSize() {

        return 0;
    }


    @Override
    public boolean checkFull() {

        return false;
    }


    @Override
    public void clear() {

    }
}
