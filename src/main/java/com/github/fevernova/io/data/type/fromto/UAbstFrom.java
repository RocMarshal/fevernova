package com.github.fevernova.io.data.type.fromto;


import com.github.fevernova.io.data.type.MethodType;
import com.github.fevernova.io.data.type.TypeException;
import lombok.Getter;
import lombok.Setter;


public abstract class UAbstFrom<M> implements UFrom {


    @Getter
    @Setter
    protected M data;

    protected MethodType methodType;


    public UAbstFrom(MethodType methodType) {

        this.methodType = methodType;
    }


    public void unsupport() {

        throw new TypeException("from type cast error");
    }

}
