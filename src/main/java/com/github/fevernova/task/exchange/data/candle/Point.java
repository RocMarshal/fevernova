package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.window.ObjectWithId;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;


public class Point extends ObjectWithId {


    private long minPrice = Long.MAX_VALUE;

    private long maxPrice = Long.MIN_VALUE;

    private long totalSize;

    private long totalCost;//sum(price * size)


    public Point(int id) {

        super(id);
    }


    public void acc(long price, long size) {

        this.minPrice = Math.min(price, this.minPrice);
        this.maxPrice = Math.max(price, this.maxPrice);
        this.totalSize += size;
        this.totalCost += price * size;
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.minPrice = bytes.readLong();
        this.maxPrice = bytes.readLong();
        this.totalSize = bytes.readLong();
        this.totalCost = bytes.readLong();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.minPrice);
        bytes.writeLong(this.maxPrice);
        bytes.writeLong(this.totalSize);
        bytes.writeLong(this.totalCost);
    }
}
