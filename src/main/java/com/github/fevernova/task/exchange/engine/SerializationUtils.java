package com.github.fevernova.task.exchange.engine;


import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;


@Slf4j
public class SerializationUtils {


    public static <T extends WriteBytesMarshallable> void writeIntHashMap(final Map<Integer, T> hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());
        hashMap.forEach((k, v) -> {
            bytes.writeInt(k);
            v.writeMarshallable(bytes);
        });
    }


    public static <T> Map<Integer, T> readIntHashMap(final BytesIn bytes, final Function<BytesIn, T> creator) {

        int length = bytes.readInt();
        final Map<Integer, T> hashMap = Maps.newHashMapWithExpectedSize(length);
        for (int i = 0; i < length; i++) {
            int k = bytes.readInt();
            hashMap.put(k, creator.apply(bytes));
        }
        return hashMap;
    }


    public static <T extends WriteBytesMarshallable> void writeList(final List<T> list, final BytesOut bytes) {

        bytes.writeInt(list.size());
        list.forEach(t -> t.writeMarshallable(bytes));
    }


    public static <T> List<T> readList(final BytesIn bytes, final Function<BytesIn, T> creator) {

        int size = bytes.readInt();
        List<T> result = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            result.add(creator.apply(bytes));
        }
        return result;
    }


    public static void writeLongArray(final long[] longs, final BytesOut bytes) {

        bytes.writeInt(longs.length);
        for (long x : longs) {
            bytes.writeLong(x);
        }
    }


    public static long[] readLongArray(BytesIn bytes) {

        int size = bytes.readInt();
        long[] result = new long[size];
        for (int i = 0; i < size; i++) {
            result[i] = bytes.readLong();
        }
        return result;
    }


    public static void writeIntArray(final int[] ints, final BytesOut bytes) {

        bytes.writeInt(ints.length);
        for (int x : ints) {
            bytes.writeInt(x);
        }
    }


    public static int[] readIntArray(BytesIn bytes) {

        int size = bytes.readInt();
        int[] result = new int[size];
        for (int i = 0; i < size; i++) {
            result[i] = bytes.readInt();
        }
        return result;
    }
}
