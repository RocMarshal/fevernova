package com.github.fevernova.task.exchange.data.uniq;


import com.github.fevernova.framework.window.ObjectWithId;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.lang3.Validate;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


@Slf4j
public class UniqIdData extends ObjectWithId {


    private final Map<Integer, RoaringBitmap> bitmaps;

    //cache
    private int currentHigh = 0;

    private RoaringBitmap currentRb;


    public UniqIdData(int id) {

        super(id);
        this.bitmaps = Maps.newHashMap();
    }


    /**
     * @param eventId
     * @return true 没有出现过 false 出现过
     */
    public boolean unique(long eventId) {

        int high = (int) (eventId >> 32);
        int low = (int) eventId;
        if (this.currentRb == null || this.currentHigh != high) {
            this.currentHigh = high;
            this.currentRb = this.bitmaps.get(high);
            if (this.currentRb == null) {
                this.currentRb = new RoaringBitmap();
                this.bitmaps.put(high, this.currentRb);
            }
        }
        return this.currentRb.checkedAdd(low);
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        int length = bytes.readInt();
        for (int i = 0; i < length; i++) {
            try {
                int key = bytes.readInt();
                int valueLen = bytes.readInt();
                ByteBuffer byteBuffer = ByteBuffer.allocate(valueLen);
                bytes.read(byteBuffer);
                byteBuffer.flip();
                RoaringBitmap rb = new RoaringBitmap();
                rb.deserialize(byteBuffer);
                this.bitmaps.put(key, rb);
            } catch (IOException e) {
                log.error("UniqIdData recovery error : ", e);
                Validate.isTrue(false);
            }
        }
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.bitmaps.size());
        this.bitmaps.forEach((k, v) -> {
            bytes.writeInt(k);
            v.runOptimize();
            int size = v.serializedSizeInBytes();
            ByteBuffer out = ByteBuffer.allocate(size);
            v.serialize(out);
            bytes.writeInt(size);
            bytes.write(out.array());
        });
    }


    public long count() {

        return this.bitmaps.entrySet().stream().mapToLong(value -> value.getValue().getLongCardinality()).sum();
    }
}
