package com.github.fevernova.task.exchange.data.depth;


import com.github.fevernova.task.exchange.SerializationUtils;
import com.google.common.collect.Lists;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;


public class DepthData implements WriteBytesMarshallable {


    @Getter
    private final List<DepthResult> data;


    public DepthData(int size) {

        this.data = Lists.newArrayListWithCapacity(size);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        SerializationUtils.writeList(this.data, bytes);

    }
}
