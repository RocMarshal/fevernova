package com.github.fevernova.framework.service.uniq;


import java.io.OutputStream;
import java.nio.ByteBuffer;


public class BrigeOut extends OutputStream {


    private ByteBuffer bb;


    public BrigeOut(ByteBuffer bb) {

        this.bb = bb;
    }


    public void write(int b) {

        bb.put((byte) b);
    }


    public void write(byte[] b) {

        bb.put(b);
    }


    public void write(byte[] b, int off, int l) {

        bb.put(b, off, l);
    }
}
