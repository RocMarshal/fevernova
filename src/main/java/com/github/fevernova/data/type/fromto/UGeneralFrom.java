package com.github.fevernova.data.type.fromto;


import com.github.fevernova.data.type.MethodType;


public abstract class UGeneralFrom<M> extends UAbstFrom<M> {


    public UGeneralFrom(MethodType methodType) {

        super(methodType);
    }


    @Override public void from(Integer p) {

        fromNumber(p);
    }


    @Override public void from(Long p) {

        fromNumber(p);
    }


    @Override public void from(Float p) {

        fromNumber(p);
    }


    @Override public void from(Double p) {

        fromNumber(p);
    }


    protected abstract void fromNumber(Number p);


    @Override public void from(String p) {

        super.data = super.methodType.parseFromString(p);
    }


    @Override public void from(byte[] p) {

        super.data = super.methodType.parseFromBytes(p);
    }
}
