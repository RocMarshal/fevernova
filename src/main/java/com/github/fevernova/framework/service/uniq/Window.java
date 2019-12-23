package com.github.fevernova.framework.service.uniq;


import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.Validate;
import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


@Slf4j
public class Window implements WriteBytesMarshallable, Comparable<Window> {


    @Getter
    private final int seq;

    private final IntObjectHashMap<RoaringBitmap> bitmaps;

    private final Map<Integer, RoaringBitmap> bitmapsCache;

    //cache
    private int currentHigh = 0;

    private RoaringBitmap currentRb;


    public Window(int seq) {

        this.seq = seq;
        this.bitmaps = new IntObjectHashMap<>();
        this.bitmapsCache = Maps.newHashMap();
    }


    public Window(final BytesIn bytes) {

        this.seq = bytes.readInt();
        int length = bytes.readInt();
        this.bitmaps = new IntObjectHashMap<>(length);
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
                log.error("Window recovery error : ", e);
                Validate.isTrue(false);
            }
        }
        this.bitmapsCache = Maps.newHashMap();
        this.bitmaps.forEachKeyValue((IntObjectProcedure<RoaringBitmap>) (each, parameter) -> bitmapsCache.put(each, parameter));
    }


    @Override public void writeMarshallable(final BytesOut bytes) {

        bytes.writeInt(this.seq);
        bytes.writeInt(this.bitmaps.size());
        this.bitmaps.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            v.runOptimize();
            int size = v.serializedSizeInBytes();
            ByteBuffer out = ByteBuffer.allocate(size);
            v.serialize(out);
            bytes.writeInt(size);
            bytes.write(out.array());
        });
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
            this.currentRb = this.bitmapsCache.get(high);
            if (this.currentRb == null) {
                this.currentRb = new RoaringBitmap();
                this.bitmaps.put(high, this.currentRb);
                this.bitmapsCache.put(high, this.currentRb);
            }
        }
        return currentRb.checkedAdd(low);
    }


    @Override public int compareTo(@NotNull Window o) {

        return Long.compare(this.seq, o.getSeq());
    }


    public long count() {

        return this.bitmaps.stream().mapToLong(value -> value.getLongCardinality()).sum();
    }
}
