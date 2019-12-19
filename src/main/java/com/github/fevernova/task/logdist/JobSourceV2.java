package com.github.fevernova.task.logdist;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.broadcast.GlobalOnceData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaverWithCoordiantor;
import com.github.fevernova.framework.service.checkpoint.ICheckPointSaver;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.kafka.KafkaConstants;
import com.github.fevernova.kafka.KafkaUtil;
import com.github.fevernova.kafka.data.KafkaCheckPoint;
import com.github.fevernova.kafka.data.KafkaData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;


@Slf4j
public class JobSourceV2 extends AbstractSource<byte[], KafkaData> implements ConsumerRebalanceListener, BarrierCoordinatorListener {


    protected ICheckPointSaver<KafkaCheckPoint> checkpoints;

    private TaskContext kafkaContext;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    private ReentrantLock kafkaLock = new ReentrantLock();

    private String topic;

    private long pollTimeOut;

    private List<TopicPartition> partitions = Lists.newArrayList();

    private boolean autoDiscovery;

    private AtomicInteger partitionsTotalNum = new AtomicInteger(0);

    private AtomicBoolean partitionsChanged = new AtomicBoolean(false);

    private boolean broadCastOnStart;


    public JobSourceV2(GlobalContext globalContext,
                       TaskContext taskContext,
                       int index,
                       int inputsNum,
                       ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.topic = super.taskContext.get(KafkaConstants.TOPICS);
        this.autoDiscovery = super.taskContext.getBoolean("autodiscovery", false);
        this.kafkaContext = new TaskContext(KafkaConstants.KAFKA, super.taskContext.getSubProperties(KafkaConstants.KAFKA_));
        this.pollTimeOut = super.taskContext.getLong(KafkaConstants.POLLTIMEOUT, 5000L);
        this.checkpoints = new CheckPointSaverWithCoordiantor<>();
        this.broadCastOnStart = super.taskContext.getBoolean("broadcastonstart", false);
    }


    @Override
    public void onStart() {

        super.onStart();
        this.kafkaConsumer = KafkaUtil.createConsumer(this.kafkaContext);
        assignPartition();
        if (this.broadCastOnStart) {
            onBroadcastData(new GlobalOnceData());
        }
    }


    private void assignPartition() {

        List<PartitionInfo> tmp = this.kafkaConsumer.partitionsFor(this.topic);
        this.partitions.clear();
        this.partitionsTotalNum.set(tmp.size());
        this.partitionsChanged.set(false);
        for (PartitionInfo partitionInfo : tmp) {
            if (super.globalContext.getJobTags().getPodIndex() == partitionInfo.partition() % super.globalContext.getJobTags().getPodTotalNum()) {
                this.partitions.add(new TopicPartition(this.topic, partitionInfo.partition()));
            }
        }
        this.kafkaConsumer.assign(this.partitions);
    }


    @Override
    public void work() {

        if (this.partitionsChanged.get()) {
            assignPartition();
        }

        ConsumerRecords<byte[], byte[]> records;
        this.kafkaLock.lock();
        try {
            records = this.kafkaConsumer.poll(this.pollTimeOut);
        } finally {
            this.kafkaLock.unlock();
        }

        if (records != null && !records.isEmpty()) {
            Set<TopicPartition> tmpPartitions = records.partitions();
            for (TopicPartition topicPartition : tmpPartitions) {
                List<ConsumerRecord<byte[], byte[]>> recordList = records.records(topicPartition);
                recordList.forEach(ele -> {
                    KafkaData data = feedOne(ele.key());
                    data.setTopic(ele.topic());
                    data.setKey(ele.key());
                    data.setValue(ele.value());
                    data.setPartitionId(ele.partition());
                    data.setTimestamp(ele.timestamp());
                    push();
                });
                super.handleRows.inc(recordList.size());
            }
        }
    }


    @Override
    protected void snapshotWhenBarrier(BarrierData barrierData) {

        KafkaCheckPoint checkPoint = new KafkaCheckPoint();
        this.kafkaLock.lock();
        try {
            this.kafkaConsumer.assignment().forEach(topicPartition -> checkPoint.put(topicPartition.topic(), topicPartition.partition(), kafkaConsumer
                    .position(new TopicPartition(topicPartition.topic(), topicPartition.partition()))));
        } catch (Throwable e) {
            log.error("JobSource Snapshot Error : ", e);
        } finally {
            this.kafkaLock.unlock();
        }
        this.checkpoints.put(barrierData.getBarrierId(), checkPoint);
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        if (log.isInfoEnabled()) {
            log.info("kafka partition update : " + JSON.toJSONString(partitions));
        }
    }


    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        if (log.isInfoEnabled()) {
            log.info("kafka partition update : " + JSON.toJSONString(partitions));
        }
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return this.checkpoints.getCheckPoint(barrierData.getBarrierId()) != null;
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) {

        KafkaCheckPoint checkPoint = this.checkpoints.getCheckPoint(barrierData.getBarrierId());
        StateValue stateValue = new StateValue();
        stateValue.setComponentType(super.componentType);
        stateValue.setComponentTotalNum(super.total);
        stateValue.setCompomentIndex(super.index);
        stateValue.setValue(Maps.newHashMap());
        stateValue.getValue().put("offsets", JSON.toJSONString(checkPoint.getOffsets()));
        return stateValue;
    }


    @Override public void onRecovery(List<StateValue> stateValues) {

        super.onRecovery(stateValues);
        for (StateValue stateValue : stateValues) {
            Map<String, Map<Integer, Long>> rs = (Map<String, Map<Integer, Long>>) JSON.parse(stateValue.getValue().get("offsets"));
            Map<TopicPartition, OffsetAndMetadata> params = Maps.newHashMap();
            rs.forEach((topic, offset) -> offset.forEach((k, v) -> params.put(new TopicPartition(topic, k), new OffsetAndMetadata(v))));
            this.kafkaLock.lock();
            try {
                this.kafkaConsumer.commitSync(params);
            } finally {
                this.kafkaLock.unlock();
            }
        }
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        KafkaCheckPoint checkPoint = this.checkpoints.remove(barrierData.getBarrierId());
        Map<String, Map<Integer, Long>> offsets = checkPoint.getOffsets();
        if (log.isInfoEnabled()) {
            log.info("commit offset : " + JSON.toJSONString(offsets));
        }
        if (offsets.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> params = Maps.newHashMap();
        offsets.forEach((topic, offset) -> offset.forEach((k, v) -> params.put(new TopicPartition(topic, k), new OffsetAndMetadata(v))));
        this.kafkaLock.lock();
        try {
            this.kafkaConsumer.commitSync(params);
            if (this.autoDiscovery) {
                try {
                    List<PartitionInfo> tmp = this.kafkaConsumer.partitionsFor(this.topic);
                    if (this.partitionsTotalNum.get() != tmp.size()) {
                        this.partitionsChanged.set(true);
                    }
                } catch (Exception e) {
                    log.error("auto discovery error : ", e);
                }
            }
        } finally {
            this.kafkaLock.unlock();
        }
    }
}
