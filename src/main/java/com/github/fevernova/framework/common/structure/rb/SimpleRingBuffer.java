package com.github.fevernova.framework.common.structure.rb;


import com.github.fevernova.framework.common.structure.rb.adj.Adjustment;
import lombok.Setter;
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

    @Setter
    private Adjustment adjustment;


    public SimpleRingBuffer(long size, long flushSize) {

        this.size = size;
        this.flushSize = flushSize;
        Validate.isTrue(Integer.bitCount((int) this.size) == 1);
        this.mask = this.size - 1;
        this.RING = new Element[(int) size];
        this.WP = new AtomicLong(0L);
        this.RP = new AtomicLong(0L);
        for (long i = 0; i < size; i++) {
            this.RING[(int) i] = (Element<E>) Element.builder().sequence(i).timestampW(System.currentTimeMillis()).timestampR
                    (System.currentTimeMillis()).accVolume(0L).build();
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
                    ele.setSequence(cur);
                    ele.setTimestampW(System.currentTimeMillis());
                    ele.setAccVolume(RING[mod(cur - 1)].getAccVolume() + eventSize);
                    ele.setBody(Optional.ofNullable(o));
                    if (this.adjustment != null) {
                        this.flushSize = this.adjustment.onEvent(ele, RING[mod(cur + 1)], (cur / this.size), sq, this.flushSize);
                        Validate.isTrue(this.flushSize > 0);
                    }
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
                    ele.setTimestampR(System.currentTimeMillis());
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
