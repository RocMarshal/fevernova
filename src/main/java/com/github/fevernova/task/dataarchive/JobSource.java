package com.github.fevernova.task.dataarchive;


import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.broadcast.GlobalOnceData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaverPlus;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.io.kafka.AbstractKafkaSource;
import com.github.fevernova.io.kafka.data.KafkaCheckPoint;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class JobSource extends AbstractKafkaSource implements BarrierCoordinatorListener {


    private List<TopicPartition> partitions = Lists.newArrayList();

    private boolean autoDiscovery;

    private AtomicInteger partitionsTotalNum = new AtomicInteger(0);

    private AtomicBoolean partitionsChanged = new AtomicBoolean(false);

    private boolean broadCastOnStart;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        super.checkpoints = new CheckPointSaverPlus<>();
        this.autoDiscovery = taskContext.getBoolean("autodiscovery", false);
        this.broadCastOnStart = taskContext.getBoolean("broadcastonstart", false);
    }


    @Override
    public void onStart() {

        super.onStart();
        assignPartition();
        if (this.broadCastOnStart) {
            onBroadcastData(new GlobalOnceData());
        }
    }


    private void assignPartition() {

        List<PartitionInfo> tmp = super.kafkaConsumer.partitionsFor(super.topics.get(0));
        this.partitions.clear();
        this.partitionsTotalNum.set(tmp.size());
        this.partitionsChanged.set(false);
        for (PartitionInfo partitionInfo : tmp) {
            if (super.globalContext.getJobTags().getPodIndex() == partitionInfo.partition() % super.globalContext.getJobTags().getPodTotalNum()) {
                this.partitions.add(new TopicPartition(super.topics.get(0), partitionInfo.partition()));
            }
        }
        super.kafkaConsumer.assign(this.partitions);
    }


    @Override
    public void work() {

        if (this.partitionsChanged.get()) {
            assignPartition();
        }
        super.work();
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return super.checkpoints.getCheckPoint(barrierData.getBarrierId()) != null;
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) throws Exception {

        KafkaCheckPoint checkPoint = super.checkpoints.getCheckPoint(barrierData.getBarrierId());
        return checkpoint2StateValue(checkPoint);
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


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        KafkaCheckPoint checkPoint = super.checkpoints.remove(barrierData.getBarrierId());
        Map<TopicPartition, OffsetAndMetadata> params = checkpoint2Params(checkPoint);
        if (params.isEmpty()) {
            return;
        }
        super.kafkaLock.lock();
        try {
            super.kafkaConsumer.commitSync(params);
            if (this.autoDiscovery) {
                try {
                    List<PartitionInfo> tmp = super.kafkaConsumer.partitionsFor(super.topics.get(0));
                    if (this.partitionsTotalNum.get() != tmp.size()) {
                        this.partitionsChanged.set(true);
                    }
                } catch (Exception e) {
                    log.error("auto discovery error : ", e);
                }
            }
        } finally {
            super.kafkaLock.unlock();
        }
    }
}
