package com.github.fevernova.data.type.impl;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.UData;
import com.github.fevernova.data.type.fromto.UGeneralFrom;
import com.github.fevernova.data.type.fromto.UNumberTo;

import java.util.Date;


public class ULong extends UData<Long> {


    public ULong(boolean lazy) {

        super(lazy);
        configure(new UGeneralFrom<Long>(MethodType.LONG) {


            @Override
            public void from(final Boolean p) {

                super.data = p ? 1L : 0L;
            }


            @Override
            public void from(final Long p) {

                super.data = p;
            }


            @Override protected void fromNumber(Number p) {

                super.data = p.longValue();
            }


            @Override
            public void from(Date p) {

                super.data = p.getTime();
            }

        }, new UNumberTo<Long>(MethodType.LONG) {


            @Override
            public Long toLong() {

                return getFromData();
            }


            @Override
            public Date toDate() {

                return new Date(getFromData());
            }


            @Override public byte[] toBytes() {

                return MethodType.LONG.convertToBytes(getFromData());
            }
        });
    }

}
