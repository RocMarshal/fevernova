package com.github.fevernova.task.marketcandle.data;


import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;


public class CandleData implements WriteBytesMarshallable, ReadBytesMarshallable {


    public static final String CONS_NAME = "CandleData";

    private Map<Integer, Line> data = Maps.newHashMap();


    public void handle(OrderMatch orderMatch, ScanFunction function) {

        Line line = getOrCreateLine(orderMatch.getSymbolId());
        line.acc(orderMatch.getTimestamp(), orderMatch.getMatchPrice(), orderMatch.getMatchSize(), orderMatch.getSequence());
        List<Point> removes = line.pollRemoved();
        if (CollectionUtils.isNotEmpty(removes)) {
            function.onChange(orderMatch.getSymbolId(), removes);
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

        this.data.forEach((id, line) -> {

            List<Point> points = line.scan4Update();
            if (CollectionUtils.isNotEmpty(points)) {
                function.onChange(id, points);
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
