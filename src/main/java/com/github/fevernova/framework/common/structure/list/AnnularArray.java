package com.github.fevernova.framework.common.structure.list;


import org.apache.commons.lang3.Validate;

import java.util.concurrent.atomic.AtomicLong;


public class AnnularArray<E> {


    private E[] elements;

    private long size;

    private AtomicLong writeCursor;


    public AnnularArray(int size) {

        this.size = size;
        this.writeCursor = new AtomicLong(0);
        this.elements = (E[]) new Object[size];
    }


    public void add(E e) {

        this.elements[(int) (this.writeCursor.getAndIncrement() % this.size)] = e;
    }


    public E get(int index) {

        Validate.isTrue(index < this.size);
        return this.elements[(int) (index % this.size)];
    }


    public E getByReverseIndex(int index) {

        if (this.writeCursor.get() > index && index < this.size) {
            return this.elements[(int) ((this.writeCursor.get() - index - 1) % this.size)];
        }
        return null;
    }
}
