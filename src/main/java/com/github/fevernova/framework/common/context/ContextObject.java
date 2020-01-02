package com.github.fevernova.framework.common.context;


public abstract class ContextObject {


    protected final GlobalContext globalContext;

    protected final TaskContext taskContext;


    public ContextObject(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
    }
}
