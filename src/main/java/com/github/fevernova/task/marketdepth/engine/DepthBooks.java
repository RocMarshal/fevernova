package com.github.fevernova.task.marketdepth.engine;


import com.github.fevernova.task.marketdepth.data.Depth;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.util.NavigableMap;


public class DepthBooks implements WriteBytesMarshallable, ReadBytesMarshallable {


    @Getter
    private NavigableMap<Long, Depth> priceTree;


    public DepthBooks(NavigableMap<Long, Depth> priceTree) {

        this.priceTree = priceTree;
    }


    public void handle(long price, long size, int count) {

        if (size == 0) {
            this.priceTree.remove(price);
        }
        Depth depth = this.priceTree.get(price);
        if (depth == null) {
            depth = new Depth();
            this.priceTree.put(price, depth);
        }
        depth.setSize(size);
        depth.setCount(count);
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        int treeSize = bytes.readInt();
        for (int i = 0; i < treeSize; i++) {
            long price = bytes.readLong();
            Depth depth = new Depth();
            depth.readMarshallable(bytes);
            this.priceTree.put(price, depth);
        }
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.priceTree.size());
        this.priceTree.forEach((price, depth) -> {
            bytes.writeLong(price);
            depth.writeMarshallable(bytes);
        });
    }
}
