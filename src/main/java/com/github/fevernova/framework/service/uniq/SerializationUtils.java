package com.github.fevernova.framework.service.uniq;


import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.function.Function;


public class SerializationUtils {


    public static <T extends WriteBytesMarshallable> void marshallLongMap(final LongObjectHashMap<T> hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());
        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            v.writeMarshallable(bytes);
        });
    }


    public static <T extends WriteBytesMarshallable> void marshallLongLongMap(final LongObjectHashMap<LongObjectHashMap<T>> hashMap,
                                                                              final BytesOut bytes) {

        bytes.writeInt(hashMap.size());
        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeLong(k);
            marshallLongMap(v, bytes);
        });
    }


    public static <T> LongObjectHashMap<T> readLongMap(final BytesIn bytes, final Function<BytesIn, T> creator) {

        int length = bytes.readInt();
        final LongObjectHashMap<T> hashMap = new LongObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            long x = bytes.readLong();
            hashMap.put(x, creator.apply(bytes));
        }
        return hashMap;
    }


    public static <T> LongObjectHashMap<LongObjectHashMap<T>> readLongLongMap(final BytesIn bytes, final Function<BytesIn, T> creator) {

        int length = bytes.readInt();
        final LongObjectHashMap<LongObjectHashMap<T>> hashMap = new LongObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            long x = bytes.readLong();
            hashMap.put(x, readLongMap(bytes, creator));
        }
        return hashMap;
    }

}
