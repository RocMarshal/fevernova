package com.github.fevernova.framework.mmap;


import lombok.Getter;

import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


@Getter
public class Buffer implements Serializable {


    /**
     * the bucket's file
     */
    private RandomAccessFile randomAccessFile = null;

    /**
     * the buffer of this file in the memory
     */
    private MappedByteBuffer mappedByteBuffer = null;

    /**
     * the buffer's size
     */
    private int bufferSize = 0;


    public Buffer(String file, int bufferSize) throws Exception {

        this.bufferSize = bufferSize;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.mappedByteBuffer = this.randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, this.bufferSize);
    }
}
