package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.window.SlideWindow;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;


public class Line extends SlideWindow<Point> {


    @Setter
    private int symbolId;

    @Setter
    private long lastSequence;


    public Line(long span, int windowNum) {

        super(span, windowNum);
    }


    public void acc(long timestamp, long price, long size) {

        prepareCurrentWindow(timestamp);
        super.currentWindow.acc(price, size);
    }


    @Override protected Point newWindow(int seq) {

        return new Point(seq);
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.symbolId = bytes.readInt();
        this.lastSequence = bytes.readLong();
        super.readMarshallable(bytes);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        bytes.writeLong(this.lastSequence);
        super.writeMarshallable(bytes);
    }
}
