package com.github.fevernova.task.performance;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.kafka.data.KafkaData;


public class JobSource extends AbstractSource<Integer, KafkaData> {


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
    }


    @Override public void work() {

        Integer t = 0;
        while (t++ < 100) {
            KafkaData data = feedOne(t);
            data.setKey(null);
            data.setValue(null);
            data.setPartitionId(t);
            data.setTimestamp(1234567890);
            push();
        }
        super.handleRows.inc(100);
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

    }
}
