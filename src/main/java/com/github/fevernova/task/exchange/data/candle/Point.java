package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.window.ObjectWithId;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;


public class Point extends ObjectWithId {


    private long startPrice;

    private long endPrice;

    private long minPrice = Long.MAX_VALUE;

    private long maxPrice = Long.MIN_VALUE;

    private long totalSize;

    private long amount;


    public Point(int id) {

        super(id);
    }


    public void acc(long price, long size) {

        if (this.totalSize == 0L) {
            this.startPrice = price;
        }
        this.endPrice = price;
        this.minPrice = Math.min(price, this.minPrice);
        this.maxPrice = Math.max(price, this.maxPrice);
        this.totalSize += size;
        this.amount += price * size;

    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.startPrice = bytes.readLong();
        this.endPrice = bytes.readLong();
        this.minPrice = bytes.readLong();
        this.maxPrice = bytes.readLong();
        this.totalSize = bytes.readLong();
        this.amount = bytes.readLong();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.startPrice);
        bytes.writeLong(this.endPrice);
        bytes.writeLong(this.minPrice);
        bytes.writeLong(this.maxPrice);
        bytes.writeLong(this.totalSize);
        bytes.writeLong(this.amount);
    }
}
