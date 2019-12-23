package com.github.fevernova.framework.component.sink;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.component.Processor;


public abstract class AbstractSink extends Processor {


    public AbstractSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

    }


    @Override protected void distributeBarrier(BarrierData barrierData) {

    }


    @Override public void onBroadcastData(BroadcastData broadcastData) {

        onBroadcast(broadcastData);
    }


    @Override protected BroadcastData onBroadcast(BroadcastData broadcastData) {

        return broadcastData;
    }
}
