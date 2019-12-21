package com.github.fevernova.framework.service.uniq;


import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.InputStreamToWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.apache.commons.lang3.Validate;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;


@Slf4j
public class SerializationUtils {


    public static <T extends WriteBytesMarshallable> void marshallIntMap(final IntObjectHashMap<T> hashMap, final BytesOut bytes) {

        bytes.writeInt(hashMap.size());
        hashMap.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            v.writeMarshallable(bytes);
        });
    }


    public static <T> IntObjectHashMap<T> readIntMap(final BytesIn bytes, final Function<BytesIn, T> creator) {

        int length = bytes.readInt();
        final IntObjectHashMap<T> hashMap = new IntObjectHashMap<>(length);
        for (int i = 0; i < length; i++) {
            int k = bytes.readInt();
            hashMap.put(k, creator.apply(bytes));
        }
        return hashMap;
    }


    public static void saveData(String pathStr, WriteBytesMarshallable obj) {

        final Path path = Paths.get(pathStr);
        log.info("Writing state to {} ...", path);
        try (final OutputStream os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
                final OutputStream bos = new BufferedOutputStream(os);
                final WireToOutputStream2 wireToOutputStream = new WireToOutputStream2(WireType.RAW, bos)) {

            final Wire wire = wireToOutputStream.getWire();
            wire.writeBytes(obj);
            log.info("done serializing, flushing {} ...", path);
            wireToOutputStream.flush();
            //bos.flush();
            log.info("completed {}", path);
        } catch (final IOException ex) {
            log.error("Can not write snapshot file: ", ex);
            Validate.isTrue(false);
        }
    }


    public static void loadData(String pathStr, ReadBytesMarshallable obj) {

        final Path path = Paths.get(pathStr);
        log.info("Loading state from {}", path);
        try (final InputStream is = Files.newInputStream(path, StandardOpenOption.READ);
                final InputStream bis = new BufferedInputStream(is)) {

            final InputStreamToWire inputStreamToWire = new InputStreamToWire(WireType.RAW, bis);
            final Wire wire = inputStreamToWire.readOne();
            log.info("start de-serializing...");
            wire.readBytes(obj);
        } catch (final IOException ex) {
            log.error("Can not read snapshot file: ", ex);
            Validate.isTrue(false);
        }
    }

}
