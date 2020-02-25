package com.github.fevernova.framework.mmap;


import java.io.IOException;


public class MMap {


    private final Buffer buffer;

    private int position = 0;


    public MMap(String file, int size) throws Exception {

        this.buffer = new Buffer(file, size);
    }


    private void require(int byteCount) throws Exception {

        int response = this.buffer.getBufferSize() - this.buffer.getMappedByteBuffer().position();
        if (response < byteCount) {
            throw new Exception("out of map buffer size limit");
        }
    }


    public void close() throws IOException {

        this.buffer.getRandomAccessFile().close();
    }


    public int getReaderPosition() {

        return this.position;
    }


    public void resetReaderPosition() {

        this.position = 0;
    }


    public int getWriterPosition() {

        return this.buffer.getMappedByteBuffer().position();
    }


    public void resetWriterPosition() {

        this.buffer.getMappedByteBuffer().position(0);
    }


    public void writeByte(byte b) throws Exception {

        this.require(1);
        this.buffer.getMappedByteBuffer().put(b);
    }


    public byte readByte() {

        byte b = this.buffer.getMappedByteBuffer().get(this.position);
        this.position++;
        return b;
    }


    public void writeBytes(byte[] bytes) throws Exception {

        this.require(bytes.length);
        this.buffer.getMappedByteBuffer().put(bytes);
    }


    public byte[] readBytes(int byteCount) {

        byte[] bytes = new byte[byteCount];
        this.buffer.getMappedByteBuffer().get(bytes);
        this.position += byteCount;
        return bytes;
    }


    public void writeInt(int i) throws Exception {

        this.require(4);
        this.buffer.getMappedByteBuffer().putInt(i);
    }


    public int readInt() {

        int i = this.buffer.getMappedByteBuffer().getInt(this.position);
        this.position += 4;
        return i;
    }


    public void writeDouble(double d) throws Exception {

        this.require(8);
        this.buffer.getMappedByteBuffer().putDouble(d);
    }


    public double readDouble() {

        double d = this.buffer.getMappedByteBuffer().getDouble(this.position);
        this.position += 8;
        return d;
    }


    public void writeLong(long l) throws Exception {

        this.require(8);
        this.buffer.getMappedByteBuffer().putLong(l);
    }


    public long readLong() {

        long l = this.buffer.getMappedByteBuffer().getLong(this.position);
        this.position += 8;
        return l;
    }
}
