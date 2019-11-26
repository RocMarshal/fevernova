package com.github.fevernova.data.type.fromto;


import com.github.fevernova.data.type.MethodType;
import com.github.fevernova.data.type.TypeException;
import lombok.Setter;


public abstract class UAbstTo<M> implements UTo {


    @Setter
    protected UAbstFrom<M> from;

    protected MethodType methodType;


    public UAbstTo(MethodType methodType) {

        this.methodType = methodType;
    }


    protected M getFromData() {

        return this.from.getData();
    }


    @Override public String toStr() {

        return this.from.getData().toString();
    }


    public void unsupport() {

        throw new TypeException("to type cast error");
    }

}
