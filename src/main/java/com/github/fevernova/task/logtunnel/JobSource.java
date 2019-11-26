package com.github.fevernova.task.logtunnel;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaverWithCoordiantor;


public class JobSource extends com.github.fevernova.task.mirrormaker.JobSource {


    public JobSource(GlobalContext globalContext,
                     TaskContext taskContext, int index, int inputsNum,
                     ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        super.checkpoints = new CheckPointSaverWithCoordiantor<>();
    }
}
