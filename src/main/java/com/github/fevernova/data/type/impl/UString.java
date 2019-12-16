package com.github.fevernova.data.type.impl;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.data.type.fromto.UAbstTo;
import com.github.fevernova.data.type.fromto.UGeneralFrom;

import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class UString extends UData<String> {


    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Charset charset;


    public UString(boolean lazy) {

        this(lazy, "UTF-8");
    }


    public UString(boolean lazy, String charsetName) {

        super(lazy);
        this.charset = Charset.forName(charsetName);
        configure(new UGeneralFrom<String>(MethodType.STRING) {


            @Override
            public void from(final Boolean p) {

                super.data = p.toString();
            }


            @Override protected void fromNumber(Number p) {

                super.data = p.toString();
            }


            @Override public void from(String p) {

                super.data = p;
            }


            @Override
            public void from(Date p) {

                super.data = format.format(p);
            }


            @Override public void from(byte[] p) {

                super.data = new String(p, charset);
            }
        }, new UAbstTo<String>(MethodType.STRING) {


            @Override
            public Boolean toBoolean() {

                return MethodType.BOOLEAN.parseFromString(getFromData());
            }


            @Override
            public Integer toInt() {

                return MethodType.INT.parseFromString(getFromData());
            }


            @Override
            public Long toLong() {

                return MethodType.LONG.parseFromString(getFromData());
            }


            @Override
            public Float toFloat() {

                return MethodType.FLOAT.parseFromString(getFromData());
            }


            @Override
            public Double toDouble() {

                return MethodType.DOUBLE.parseFromString(getFromData());
            }


            @Override
            public Date toDate() {

                try {
                    return format.parse(getFromData());
                } catch (ParseException e) {
                    unsupport();
                }
                return null;
            }


            @Override public byte[] toBytes() {

                return getFromData().getBytes(charset);
            }
        });
    }

}
