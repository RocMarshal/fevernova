package com.github.fevernova.data.type.impl;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.data.type.fromto.UGeneralFrom;
import com.github.fevernova.data.type.fromto.UNumberTo;

import java.util.Date;


public class UFloat extends UData<Float> {


    public UFloat(boolean lazy) {

        super(lazy);
        configure(new UGeneralFrom<Float>(MethodType.FLOAT) {


            @Override
            public void from(Boolean p) {

                super.data = p ? 1f : 0f;
            }


            @Override
            public void from(Float p) {

                super.data = p;
            }


            @Override public void from(Date p) {

                super.data = Float.valueOf(p.getTime());
            }


            @Override protected void fromNumber(Number p) {

                super.data = p.floatValue();
            }

        }, new UNumberTo<Float>(MethodType.FLOAT) {


            @Override
            public Float toFloat() {

                return getFromData();
            }


            @Override public Date toDate() {

                return new Date(getFromData().longValue() * 1000l);
            }


            @Override public byte[] toBytes() {

                return MethodType.FLOAT.convertToBytes(getFromData());
            }
        });
    }
}
