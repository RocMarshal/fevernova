package com.github.fevernova.framework.common.structure.rb;


import java.io.Serializable;
import java.util.Optional;


public interface IRingBuffer<E> extends Serializable {


    boolean add(E e, long size);

    Optional<E> get();

    int curSize();

    boolean checkFull();

    void clear();

}
