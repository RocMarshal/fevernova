package com.github.fevernova.data.type.fromto;


import com.github.fevernova.data.type.MethodType;


public abstract class UNumberTo<M extends Number> extends UAbstTo<M> {


    public UNumberTo(MethodType methodType) {

        super(methodType);
    }


    @Override public Boolean toBoolean() {

        return getFromData().intValue() > 0 ? Boolean.TRUE : Boolean.FALSE;
    }


    @Override public Integer toInt() {

        return getFromData().intValue();
    }


    @Override public Long toLong() {

        return getFromData().longValue();
    }


    @Override public Float toFloat() {

        return getFromData().floatValue();
    }


    @Override public Double toDouble() {

        return getFromData().doubleValue();
    }
}
