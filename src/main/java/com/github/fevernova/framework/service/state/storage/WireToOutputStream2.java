package com.github.fevernova.framework.service.state.storage;


import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


public class WireToOutputStream2 implements AutoCloseable {


    private final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer(128 * 1024 * 1024);

    private final Wire wire;

    private final DataOutputStream dos;


    public WireToOutputStream2(WireType wireType, OutputStream os) {

        wire = wireType.apply(bytes);
        dos = new DataOutputStream(os);
    }


    public Wire getWire() {

        wire.clear();
        return wire;
    }


    public void flush() throws IOException {

        int length = Math.toIntExact(bytes.readRemaining());
        dos.writeInt(length);

        final byte[] buf = new byte[1024 * 1024];

        while (bytes.readPosition() < bytes.readLimit()) {
            int read = bytes.read(buf);
            dos.write(buf, 0, read);
        }
    }


    @Override
    public void close() {

        bytes.release();
    }
}
