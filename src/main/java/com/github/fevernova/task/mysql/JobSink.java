package com.github.fevernova.task.mysql;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractSink;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JobSink extends AbstractSink {


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
    }


    @Override protected void handleEvent(Data event) {

    }

}
