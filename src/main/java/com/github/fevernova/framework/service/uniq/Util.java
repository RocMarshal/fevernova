package com.github.fevernova.framework.service.uniq;


import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.InputStreamToWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.apache.commons.lang3.Validate;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


@Slf4j
public class Util {


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
