package com.github.fevernova.data.type.impl;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.data.type.fromto.UGeneralFrom;
import com.github.fevernova.data.type.fromto.UNumberTo;

import java.util.Date;


public class UDouble extends UData<Double> {


    public UDouble(boolean lazy) {

        super(lazy);
        configure(new UGeneralFrom<Double>(MethodType.DOUBLE) {


            @Override
            public void from(Boolean p) {

                super.data = p ? 1d : 0d;
            }


            @Override
            public void from(Double p) {

                super.data = p;
            }


            @Override public void from(Date p) {

                super.data = Double.valueOf(p.getTime());
            }


            @Override protected void fromNumber(Number p) {

                super.data = p.doubleValue();
            }

        }, new UNumberTo<Double>(MethodType.DOUBLE) {


            @Override
            public Double toDouble() {

                return getFromData();
            }


            @Override public Date toDate() {

                return new Date(getFromData().longValue());
            }


            @Override public byte[] toBytes() {

                return MethodType.DOUBLE.convertToBytes(getFromData());
            }
        });
    }
}
