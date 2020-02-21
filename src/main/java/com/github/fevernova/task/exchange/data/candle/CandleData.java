package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;


public class CandleData implements WriteBytesMarshallable, ReadBytesMarshallable {


    @Getter
    private Map<Integer, Line> data;


    public CandleData(int size) {

        this.data = Maps.newHashMapWithExpectedSize(size);
    }


    public void acc(int symbolId, long timestamp, long price, long size, long sequence, ScanFunction function) {

        Line line = getOrCreateLine(symbolId);
        line.acc(timestamp, price, size, sequence);
        Point removed = line.pollRemoved();
        if (removed != null) {
            function.onRemove(symbolId, Lists.newArrayList(removed));
        }
    }


    private Line getOrCreateLine(int symbolId) {

        Line line = this.data.get(symbolId);
        if (line == null) {
            line = new Line(60 * 1000L, 3);
            this.data.put(symbolId, line);
        }
        return line;
    }


    public void scan4Update(ScanFunction function) {

        this.data.forEach((integer, line) -> {

            List<Point> points = line.scan4Update();
            if (!points.isEmpty()) {
                function.onUpdate(integer, points);
            }
        });
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(0);
        SerializationUtils.writeIntHashMap(this.data, bytes);
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        Validate.isTrue(bytes.readInt() == 0);
        this.data = SerializationUtils.readIntHashMap(bytes, bytesIn -> {

            Line line = new Line(60 * 1000, 3);
            line.readMarshallable(bytesIn);
            return line;
        });
    }
}
