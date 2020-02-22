package com.github.fevernova.task.marketcandle.data;


import com.github.fevernova.task.exchange.window.ObjectWithId;
import lombok.Getter;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;


@Getter
public class Point extends ObjectWithId {


    private long startPrice;

    private long endPrice;

    private long minPrice = Long.MAX_VALUE;

    private long maxPrice = Long.MIN_VALUE;

    private long totalSize;

    private long amount;

    private int count;

    private long firstSequence = Long.MAX_VALUE;

    private long lastSequence = Long.MIN_VALUE;

    @Setter
    private boolean completed = false;

    @Setter
    private boolean update = false;


    public Point(int id) {

        super(id);
    }


    public void acc(long price, long size, long sequence) {

        if (this.lastSequence >= sequence) {
            return;
        }
        this.startPrice = this.totalSize == 0L ? price : this.startPrice;
        this.endPrice = price;
        this.minPrice = Math.min(price, this.minPrice);
        this.maxPrice = Math.max(price, this.maxPrice);
        this.totalSize += size;
        this.amount += price * size;
        this.count++;
        this.firstSequence = this.totalSize == 0L ? price : sequence;
        this.lastSequence = sequence;
        this.update = true;
    }


    public Point copyByScan() {

        Point point = new Point(getId());
        point.startPrice = this.startPrice;
        point.endPrice = this.endPrice;
        point.minPrice = this.minPrice;
        point.maxPrice = this.maxPrice;
        point.totalSize = this.totalSize;
        point.amount = this.amount;
        point.count = this.count;
        point.firstSequence = this.firstSequence;
        point.lastSequence = this.lastSequence;
        this.update = false;
        return point;
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.startPrice = bytes.readLong();
        this.endPrice = bytes.readLong();
        this.minPrice = bytes.readLong();
        this.maxPrice = bytes.readLong();
        this.totalSize = bytes.readLong();
        this.amount = bytes.readLong();
        this.count = bytes.readInt();
        this.firstSequence = bytes.readLong();
        this.lastSequence = bytes.readLong();
        this.update = bytes.readBoolean();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.startPrice);
        bytes.writeLong(this.endPrice);
        bytes.writeLong(this.minPrice);
        bytes.writeLong(this.maxPrice);
        bytes.writeLong(this.totalSize);
        bytes.writeLong(this.amount);
        bytes.writeInt(this.count);
        bytes.writeLong(this.firstSequence);
        bytes.writeLong(this.lastSequence);
        bytes.writeBoolean(this.update);
    }
}
