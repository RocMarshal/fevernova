package com.github.fevernova.task.exchange.engine;


import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

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
}
