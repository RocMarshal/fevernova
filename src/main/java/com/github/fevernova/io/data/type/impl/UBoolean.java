package com.github.fevernova.io.data.type.impl;


import com.github.fevernova.io.data.type.MethodType;
import com.github.fevernova.io.data.type.UData;
import com.github.fevernova.io.data.type.fromto.UAbstTo;
import com.github.fevernova.io.data.type.fromto.UGeneralFrom;

import java.util.Date;


public class UBoolean extends UData<Boolean> {


    public UBoolean(boolean lazy) {

        super(lazy);
        configure(new UGeneralFrom<Boolean>(MethodType.BOOLEAN) {


            @Override
            public void from(final Boolean p) {

                super.data = p;
            }


            @Override public void from(Date p) {

                unsupport();
            }


            @Override protected void fromNumber(Number p) {

                super.data = (p.intValue() != 0 ? Boolean.TRUE : Boolean.FALSE);
            }
        }, new UAbstTo<Boolean>(MethodType.BOOLEAN) {


            @Override
            public Boolean toBoolean() {

                return getFromData();
            }


            @Override
            public Integer toInt() {

                return getFromData() ? 1 : 0;
            }


            @Override
            public Long toLong() {

                return getFromData() ? 1L : 0L;
            }


            @Override
            public Float toFloat() {

                return getFromData() ? 1f : 0f;
            }


            @Override
            public Double toDouble() {

                return getFromData() ? 1d : 0d;
            }


            @Override public Date toDate() {

                unsupport();
                return null;
            }


            @Override public byte[] toBytes() {

                return MethodType.BOOLEAN.convertToBytes(getFromData());
            }
        });
    }

}
