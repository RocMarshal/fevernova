package com.github.fevernova.io.data.type.impl;


import com.github.fevernova.io.data.type.MethodType;
import com.github.fevernova.io.data.type.UData;
import com.github.fevernova.io.data.type.fromto.UAbstFrom;
import com.github.fevernova.io.data.type.fromto.UAbstTo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public abstract class UAbstDate extends UData<Date> {


    private final SimpleDateFormat format;


    public UAbstDate(boolean lazy) {

        super(lazy);
        this.format = new SimpleDateFormat(getFormatString());
        configure(new UAbstFrom<Date>(MethodType.DATE) {


            @Override
            public void from(final Boolean p) {

                unsupport();
            }


            @Override
            public void from(final Integer p) {

                super.data = new Date(p * 1000L);
            }


            @Override
            public void from(final Long p) {

                super.data = new Date(p);
            }


            @Override
            public void from(final Float p) {

                super.data = new Date(p.longValue() * 1000L);
            }


            @Override
            public void from(final Double p) {

                super.data = new Date(p.longValue());

            }


            @Override
            public void from(final String p) {

                try {
                    super.data = format.parse(p);
                } catch (ParseException e) {
                    unsupport();
                }
            }


            @Override
            public void from(Date p) {

                super.data = p;
            }


            @Override public void from(byte[] p) {

                from((Long) MethodType.LONG.parseFromBytes(p));
            }
        }, new UAbstTo<Date>(MethodType.DATE) {


            @Override
            public Boolean toBoolean() {

                unsupport();
                return null;
            }


            @Override
            public Integer toInt() {

                return (int) (getFromData().getTime() / 1000L);
            }


            @Override
            public Long toLong() {

                return getFromData().getTime();
            }


            @Override
            public Float toFloat() {

                return Float.valueOf(getFromData().getTime() / 1000L);
            }


            @Override
            public Double toDouble() {

                return Double.valueOf(getFromData().getTime());
            }


            @Override
            public String toStr() {

                return format.format(getFromData());
            }


            @Override
            public Date toDate() {

                return getFromData();
            }


            @Override public byte[] toBytes() {

                return MethodType.LONG.convertToBytes(toLong());
            }
        });
    }


    protected abstract String getFormatString();

}
