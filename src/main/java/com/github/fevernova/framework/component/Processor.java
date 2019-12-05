package com.github.fevernova.framework.component;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataEvent;
import com.github.fevernova.framework.common.data.DataType;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.component.channel.WaitNotify;
import com.lmax.disruptor.TimeoutHandler;
import com.lmax.disruptor.WorkHandler;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class Processor extends Component implements WorkHandler<DataEvent>, TimeoutHandler, WaitNotify {


    public Processor(GlobalContext globalContext,
                     TaskContext taskContext,
                     int index,
                     int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
    }


    @Override
    public void onEvent(DataEvent event) {

        if (event.getDataType() == DataType.DATA) {
            super.handleRows.inc();
            handleEvent(event.getData());
            event.getData().clearData();
        } else if (event.getDataType() == DataType.BARRIER) {
            onBarrierData(((BarrierData) event.getData()));
        } else if (event.getDataType() == DataType.BROADCAST) {
            onBroadcastData((BroadcastData) event.getData());
        } else {
            super.globalContext.fatalError("Unkonwn DataEvent Type");
        }
    }


    protected abstract void handleEvent(Data event);


    @Override public void onTimeout(long sequence) {

        timeOut();
    }


    protected void timeOut() {

    }


    @Override public void waitTime(long waitNS) {

        this.waitTime.inc(waitNS);
    }
}
