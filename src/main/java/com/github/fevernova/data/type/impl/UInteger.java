package com.github.fevernova.data.type.impl;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.data.type.fromto.UGeneralFrom;
import com.github.fevernova.data.type.fromto.UNumberTo;

import java.util.Date;


public class UInteger extends UData<Integer> {


    public UInteger(boolean lazy) {

        super(lazy);
        configure(new UGeneralFrom<Integer>(MethodType.INT) {


            @Override
            public void from(final Boolean p) {

                super.data = p ? 1 : 0;
            }


            @Override
            public void from(final Integer p) {

                super.data = p;
            }


            @Override protected void fromNumber(Number p) {

                super.data = p.intValue();
            }


            @Override
            public void from(Date p) {

                super.data = (int) (p.getTime() / 1000);
            }

        }, new UNumberTo<Integer>(MethodType.INT) {


            @Override
            public Integer toInt() {

                return getFromData();
            }


            @Override
            public Date toDate() {

                return new Date(getFromData() * 1000L);
            }


            @Override public byte[] toBytes() {

                return MethodType.INT.convertToBytes(getFromData());
            }
        });
    }

}
