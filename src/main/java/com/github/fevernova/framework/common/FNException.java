package com.github.fevernova.framework.common;


public class FNException extends RuntimeException {


    private static final long serialVersionUID = 1L;


    public FNException(String msg) {

        super(msg);
    }


    public FNException(String msg, Throwable th) {

        super(msg, th);
    }


    public FNException(Throwable th) {

        super(th);
    }

}
