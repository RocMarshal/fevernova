package com.github.fevernova.framework.component.source;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.barrier.listener.BarrierCompletedListener;


public abstract class AbstractBatchSource<K, V extends Data> extends AbstractSource<K, V> implements BarrierCompletedListener {


    private boolean jobFinished = false;

    protected Long lastBarrierId;

    protected long startTime;

    protected long endTime;


    public AbstractBatchSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.startTime = Util.nowMS();
    }


    protected void jobFinished() {

        this.jobFinished = true;
        this.endTime = Util.nowMS();
        super.onPause();
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

        if (this.jobFinished && this.lastBarrierId == null) {
            this.lastBarrierId = barrierData.getBarrierId();
        }
    }


    @Override public void completed(BarrierData barrierData) throws Exception {

        if (this.jobFinished && this.lastBarrierId != null && this.lastBarrierId <= barrierData.getBarrierId()) {
            jobFinishedListener();
            super.globalContext.jobFinished();
        }
    }


    public abstract void jobFinishedListener();
}
