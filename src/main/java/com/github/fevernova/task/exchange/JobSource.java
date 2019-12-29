package com.github.fevernova.task.exchange;


import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.io.kafka.AbstractKafkaSource;
import com.github.fevernova.io.kafka.data.KafkaCheckPoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;


@Slf4j
public class JobSource extends AbstractKafkaSource implements BarrierCoordinatorListener {


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        super.checkpoints = new CheckPointSaver<>();
    }


    @Override public void onStart() {

        super.onStart();
        super.kafkaConsumer.subscribe(super.topics, this);
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return super.checkpoints.getCheckPoint(barrierData.getBarrierId()) != null;
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) throws Exception {

        KafkaCheckPoint checkPoint = super.checkpoints.getCheckPoint(barrierData.getBarrierId());
        return checkpoint2StateValue(checkPoint);
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        KafkaCheckPoint checkPoint = super.checkpoints.remove(barrierData.getBarrierId());
        Map<TopicPartition, OffsetAndMetadata> params = checkpoint2Params(checkPoint);
        if (!params.isEmpty()) {
            commitKafkaOffset(params);
        }
    }


    @Override public void onRecovery(List<StateValue> stateValues) {

        super.onRecovery(stateValues);
        for (StateValue stateValue : stateValues) {
            KafkaCheckPoint checkPoint = new KafkaCheckPoint();
            checkPoint.parseFromJSON((JSONObject) stateValue.getValue());
            Map<TopicPartition, OffsetAndMetadata> params = checkpoint2Params(checkPoint);
            if (!params.isEmpty()) {
                commitKafkaOffset(params);
            }
        }
    }
}
