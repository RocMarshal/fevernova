package com.github.fevernova.framework.component;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.lmax.disruptor.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PExceptionHandler<T> implements ExceptionHandler<T> {


    private GlobalContext globalContext;


    public PExceptionHandler(GlobalContext globalContext) {

        this.globalContext = globalContext;
    }


    @Override
    public void handleEventException(final Throwable ex, final long sequence, final T event) {

        log.error("Exception processing: " + sequence + " " + event, ex);
        this.globalContext.fatalError("Handle Event Exception .", ex);
        throw new RuntimeException(ex);
    }


    @Override
    public void handleOnStartException(final Throwable ex) {

        this.globalContext.fatalError("Handle Event Start Exception .", ex);
        log.error("Exception during onStart()", ex);
    }


    @Override
    public void handleOnShutdownException(final Throwable ex) {

        this.globalContext.fatalError("Handle Event Shutdown Exception .", ex);
        log.error("Exception during onShutdown()", ex);
    }

}
