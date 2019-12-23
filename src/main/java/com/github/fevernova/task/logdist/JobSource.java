package com.github.fevernova.task.logdist;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.broadcast.GlobalOnceData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaverPlus;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JobSource extends com.github.fevernova.task.mirrormaker.JobSource {


    private boolean broadCastOnStart;


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
}
