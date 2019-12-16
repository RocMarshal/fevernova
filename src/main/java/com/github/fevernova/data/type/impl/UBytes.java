package com.github.fevernova.data.type.impl;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.data.type.fromto.UAbstFrom;
import com.github.fevernova.data.type.fromto.UAbstTo;

import java.nio.charset.Charset;
import java.util.Date;


public class UBytes extends UData<byte[]> {


    private Charset charset;


    public UBytes(boolean lazy) {

        this(lazy, "UTF-8");
    }


    public UBytes(boolean lazy, String charsetName) {

        super(lazy);
        this.charset = Charset.forName(charsetName);
        configure(new UAbstFrom<byte[]>(MethodType.BYTES) {


            @Override public void from(Boolean p) {

                super.data = MethodType.BOOLEAN.convertToBytes(p);
            }


            @Override public void from(Integer p) {

                super.data = MethodType.INT.convertToBytes(p);
            }


            @Override public void from(Long p) {

                super.data = MethodType.LONG.convertToBytes(p);
            }


            @Override public void from(Float p) {

                super.data = MethodType.FLOAT.convertToBytes(p);
            }


            @Override public void from(Double p) {

                super.data = MethodType.DOUBLE.convertToBytes(p);
            }


            @Override public void from(String p) {

                super.data = p.getBytes(charset);
            }


            @Override public void from(Date p) {

                from(p.getTime());
            }


            @Override public void from(byte[] p) {

                super.data = p;
            }
        }, new UAbstTo<byte[]>(MethodType.BYTES) {


            @Override public Boolean toBoolean() {

                return MethodType.BOOLEAN.parseFromBytes(getFromData());
            }


            @Override public Integer toInt() {

                return MethodType.INT.parseFromBytes(getFromData());
            }


            @Override public Long toLong() {

                return MethodType.LONG.parseFromBytes(getFromData());
            }


            @Override public Float toFloat() {

                return MethodType.FLOAT.parseFromBytes(getFromData());
            }


            @Override public Double toDouble() {

                return MethodType.DOUBLE.parseFromBytes(getFromData());
            }


            @Override public String toStr() {

                return new String(getFromData(), charset);
            }


            @Override public Date toDate() {

                return new Date(toLong());
            }


            @Override public byte[] toBytes() {

                return getFromData();
            }
        });
    }
}
