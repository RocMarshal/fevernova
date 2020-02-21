package com.github.fevernova.task.marketdetail;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.barrier.listener.BarrierCompletedListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.io.kafka.AbstractKafkaSource;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JobSource extends AbstractKafkaSource implements BarrierCompletedListener {


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        super.checkpoints = new CheckPointSaver<>();
    }


    @Override
    public void onStart() {

        super.onStart();
        super.kafkaConsumer.subscribe(super.topics, this);
    }


    @Override
    public void completed(BarrierData barrierData) throws Exception {

        super.checkpoints.remove(barrierData.getBarrierId());
    }
}
