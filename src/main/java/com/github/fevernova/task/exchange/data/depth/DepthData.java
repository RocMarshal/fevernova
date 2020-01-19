package com.github.fevernova.task.exchange.data.depth;


import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.google.common.collect.Lists;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;


public class DepthData implements WriteBytesMarshallable {


    @Getter
    private final List<DepthResult> data;


    public DepthData(int size) {

        this.data = Lists.newArrayListWithCapacity(size);
    }


    public DepthData(BytesIn bytes) {

        this.data = SerializationUtils.readList(bytes, bytesIn -> {

            DepthResult depthResult = new DepthResult();
            depthResult.readMarshallable(bytes);
            return depthResult;
        });
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        SerializationUtils.writeList(this.data, bytes);
    }
}
