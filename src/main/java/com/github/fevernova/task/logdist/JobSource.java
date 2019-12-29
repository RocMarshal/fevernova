package com.github.fevernova.task.logdist;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.broadcast.GlobalOnceData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaverPlus;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.io.kafka.data.KafkaCheckPoint;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
public class JobSource extends com.github.fevernova.task.mirrormaker.JobSource implements BarrierCoordinatorListener {


    private boolean broadCastOnStart;

    private final AtomicBoolean canBeCommitted = new AtomicBoolean(false);


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        super.checkpoints = new CheckPointSaverPlus<>();
        this.broadCastOnStart = super.taskContext.getBoolean("broadcastonstart", false);
    }


    @Override
    public void onStart() {

        super.onStart();
        if (this.broadCastOnStart) {
            onBroadcastData(new GlobalOnceData());
        }
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return super.checkpoints.getCheckPoint(barrierData.getBarrierId()) != null;
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) {

        KafkaCheckPoint checkPoint = super.checkpoints.getCheckPoint(barrierData.getBarrierId());
        return checkpoint2StateValue(checkPoint);
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        this.canBeCommitted.set(result);
    }


    @Override public void completed(BarrierData barrierData) throws Exception {

        if (this.canBeCommitted.get()) {
            super.completed(barrierData);
        }
    }
}
