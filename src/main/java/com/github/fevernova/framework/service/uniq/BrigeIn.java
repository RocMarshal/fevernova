package com.github.fevernova.framework.service.uniq;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


public class BrigeIn extends InputStream {


    private ByteBuffer bb;


    public BrigeIn(ByteBuffer bb) {

        this.bb = bb;
    }


    @Override public int read() throws IOException {

        return bb.get();
    }
}
