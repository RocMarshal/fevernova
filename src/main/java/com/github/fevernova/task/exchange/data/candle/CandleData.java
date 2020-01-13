package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.google.common.collect.Lists;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;


public class CandleData implements WriteBytesMarshallable {


    @Getter
    private final List<Line> data;


    public CandleData(int size) {

        this.data = Lists.newArrayListWithCapacity(size);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        SerializationUtils.writeList(this.data, bytes);
    }

}
