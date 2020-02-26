package com.github.fevernova.task.mysql;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractBatchSink;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;


@Slf4j
public class JobSink extends AbstractBatchSink {


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
    }


    @Override protected void prepare(Data event) {

    }


    @Override protected int handleEventAndReturnSize(Data dataEvent) {

        return 0;
    }


    @Override protected void close() throws Exception {

    }


    @Override protected void sendBatch() throws IOException {

    }


    @Override protected void snapshotWhenBarrierAfterBatch(BarrierData barrierData) {

    }

}
