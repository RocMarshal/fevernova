package com.github.fevernova.framework.common;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;


public interface Configurable {


    default void configure(GlobalContext globalContext, TaskContext context) {

    }

    default void configure(TaskContext context) {

    }
}
