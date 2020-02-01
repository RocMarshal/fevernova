package com.github.fevernova.task.exchange.data.depth;


import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.google.common.collect.Maps;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Map;


public class DepthData implements WriteBytesMarshallable {


    @Getter
    private final Map<Integer, DepthResult> data;


    public DepthData(int size) {

        this.data = Maps.newHashMapWithExpectedSize(size);
    }


    public DepthData(BytesIn bytes) {

        this.data = SerializationUtils.readIntHashMap(bytes, bytesIn -> {

            DepthResult depthResult = new DepthResult();
            depthResult.readMarshallable(bytes);
            return depthResult;
        });
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        SerializationUtils.writeIntHashMap(this.data, bytes);
    }
}
