package com.github.fevernova.framework.common.structure.rb;


import org.apache.commons.lang3.Validate;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;


public class SimpleRingBuffer<E> implements IRingBuffer<E> {


    private Element<E>[] RING;

    private long size;

    private long mask;

    //将要写入该位置
    private AtomicLong WP;

    //即将读取该位置
    private AtomicLong RP;

    private long flushSize;


    public SimpleRingBuffer(long size, long flushSize) {

        this.size = size;
        this.flushSize = flushSize;
        Validate.isTrue(Integer.bitCount((int) this.size) == 1);
        this.mask = this.size - 1;
        this.RING = new Element[(int) size];
        this.WP = new AtomicLong(0L);
        this.RP = new AtomicLong(0L);
        for (long i = 0; i < size; i++) {
            this.RING[(int) i] = (Element<E>) Element.builder().build();
        }
    }


    public SimpleRingBuffer(long size) {

        this(size, size);
    }


    @Override
    public boolean add(E o, long eventSize) {

        for (; ; ) {
            long cur = WP.get();
            if (cur - RP.get() == this.size - 1) {
                return false;
            } else {
                if (WP.compareAndSet(cur, cur + 1)) {
                    int sq = mod(cur);
                    Element<E> ele = RING[sq];
                    ele.setBody(Optional.ofNullable(o));
                    return true;
                }
            }
        }
    }


    @Override
    public Optional<E> get() {

        for (; ; ) {
            long cur = RP.get();
            if (cur == WP.get()) {
                return null;
            } else {
                if (RP.compareAndSet(cur, cur + 1)) {
                    Element<E> ele = RING[mod(cur)];
                    Optional<E> r = ele.getBody();
                    if (r == null) {
                        while (r == null) {
                            Thread.yield();
                            r = ele.getBody();
                        }
                    }
                    ele.setBody(null);
                    return r;
                }
            }
        }
    }


    @Override
    public boolean checkFull() {

        return WP.get() - RP.get() == this.flushSize;
    }


    @Override
    public void clear() {

        while (get() != null) {
            // nothing
        }
    }


    @Override
    public int curSize() {

        return (int) (WP.get() - RP.get());
    }


    private int mod(long i) {

        return (int) (i & mask);
    }
}
