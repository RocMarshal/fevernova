package com.github.fevernova.task.marketcandle.data;


import com.google.common.collect.Lists;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;


public class CandleLine implements WriteBytesMarshallable, ReadBytesMarshallable {


    private static final long SPAN = 60 * 1000L;

    private Point first;

    private Point second;

    private Point third;

    private int symbolId;


    public CandleLine(int symbolId) {

        this.symbolId = symbolId;
    }


    public void acc(long timestamp, long price, long size, long sequence, INotify notify) {

        int seq = (int) (timestamp / SPAN);
        Point point = locate(seq, notify);
        if (point == null) {
            return;
        }
        point.acc(price, size, sequence);
    }


    public void scan4Update(boolean repair, INotify notify, long now) {

        if (repair && this.first != null) {
            if (((now - SPAN * this.first.getId()) > 90 * 1000L) && ((now - this.first.getUpdateTime()) > 15 * 1000L)) {
                locate(this.first.getId() + 1, notify);
            }
        }
        final List<Point> result = Lists.newArrayList();
        if (this.first != null && this.first.isUpdate()) {
            result.add(this.first.copyByScan());
        }
        if (this.second != null && this.second.isUpdate()) {
            result.add(this.second.copyByScan());
        }
        if (this.third != null && this.third.isUpdate()) {
            result.add(this.third.copyByScan());
        }
        if (CollectionUtils.isNotEmpty(result)) {
            notify.onChange(this.symbolId, result);
        }
    }


    private Point locate(int seq, INotify notify) {

        if (this.first == null) {
            this.first = new Point(seq);
            return this.first;
        }

        if (this.first.getId() == seq) {
            return this.first;
        } else if (this.first.getId() < seq) {
            do {
                Point point = new Point(this.first.getId() + 1);
                point.initPrice(this.first.getEndPrice(), this.first.getLastSequence());
                rolling(notify);
                this.first = point;
            } while (this.first.getId() < seq);
            return this.first;
        } else {
            if (this.second == null || this.second.getId() == seq) {
                return this.second;
            }
            if (this.third == null || this.third.getId() == seq) {
                return this.third;
            }
            return null;
        }
    }


    private void rolling(INotify notify) {

        Point result = this.third;
        this.third = this.second;
        this.second = this.first;
        this.first = null;
        if (result != null) {
            notify.onChange(this.symbolId, Lists.newArrayList(result));
        }
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.symbolId = bytes.readInt();
        int size = bytes.readInt();
        switch (size) {
            case 3:
                this.third = new Point(bytes.readInt());
                this.third.readMarshallable(bytes);
            case 2:
                this.second = new Point(bytes.readInt());
                this.second.readMarshallable(bytes);
            case 1:
                this.first = new Point(bytes.readInt());
                this.first.readMarshallable(bytes);
            case 0:
        }
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        int size = bool2int(this.first) + bool2int(this.second) + bool2int(this.third);
        bytes.writeInt(size);
        if (this.first != null) {
            bytes.writeInt(this.first.getId());
            this.first.writeMarshallable(bytes);
        }
        if (this.second != null) {
            bytes.writeInt(this.second.getId());
            this.second.writeMarshallable(bytes);
        }
        if (this.third != null) {
            bytes.writeInt(this.third.getId());
            this.third.writeMarshallable(bytes);
        }
    }


    private int bool2int(Point point) {

        return point == null ? 0 : 1;
    }
}
