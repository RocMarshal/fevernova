package com.github.fevernova.framework.service.uniq;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.Validate;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;


@Slf4j
public class Window implements WriteBytesMarshallable, Comparable<Window> {


    @Getter
    private final long seq;

    private final IntObjectHashMap<RoaringBitmap> bitmaps;


    public Window(long seq) {

        this.seq = seq;
        this.bitmaps = new IntObjectHashMap<>();
    }


    public Window(BytesIn bytes) {

        this.seq = bytes.readLong();

        int length = bytes.readInt();
        this.bitmaps = new IntObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            int key = bytes.readInt();
            int valueLen = bytes.readInt();
            ByteBuffer bb = ByteBuffer.allocate(valueLen);
            bytes.read(bb);
            bb.flip();
            RoaringBitmap rb = new RoaringBitmap();
            try {
                rb.deserialize(bb);
            } catch (IOException e) {
                log.error("Window recovery error : ", e);
                Validate.isTrue(false);
            }
            this.bitmaps.put(key, rb);
        }
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.seq);
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
    public boolean uniq(long eventId) {

        int high = (int) (eventId >> 32);
        int low = (int) eventId;

        RoaringBitmap rb = this.bitmaps.get(high);
        if (rb == null) {
            rb = new RoaringBitmap();
            this.bitmaps.put(high, rb);
        }

        boolean r = rb.contains(low);
        if (!r) {
            rb.add(low);
        }
        return !r;
    }


    @Override public int compareTo(@NotNull Window o) {

        return Long.compare(this.seq, o.getSeq());
    }
}
