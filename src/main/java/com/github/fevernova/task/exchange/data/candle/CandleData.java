package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.google.common.collect.Maps;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.Validate;

import java.util.Map;


public class CandleData implements WriteBytesMarshallable {


    @Getter
    private final Map<Integer, Line> data;


    public CandleData(int size) {

        this.data = Maps.newHashMapWithExpectedSize(size);
    }


    public CandleData(BytesIn bytes, long span, int windowNum) {

        Validate.isTrue(bytes.readInt() == 0);
        this.data = SerializationUtils.readIntHashMap(bytes, bytesIn -> {

            Line line = new Line(span, windowNum);
            line.readMarshallable(bytesIn);
            return line;
        });
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(0);
        SerializationUtils.writeIntHashMap(this.data, bytes);
    }

}
