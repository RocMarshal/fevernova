package com.github.fevernova.task.mirrormaker;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.service.barrier.listener.BarrierCompletedListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.framework.service.checkpoint.ICheckPointSaver;
import com.github.fevernova.kafka.KafkaConstants;
import com.github.fevernova.kafka.KafkaUtil;
import com.github.fevernova.kafka.data.KafkaCheckPoint;
import com.github.fevernova.kafka.data.KafkaData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;


@Slf4j
public class JobSource extends AbstractSource<byte[], KafkaData> implements ConsumerRebalanceListener, BarrierCompletedListener {


    protected ICheckPointSaver<KafkaCheckPoint> checkpoints;

    private TaskContext kafkaContext;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    private ReentrantLock kafkaLock = new ReentrantLock();

    private String topic;

    private long pollTimeOut;

    private List<TopicPartition> partitions = Lists.newArrayList();


    public JobSource(GlobalContext globalContext,
                     TaskContext taskContext,
                     int index,
                     int inputsNum,
                     ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.topic = super.taskContext.get("topic");
        this.kafkaContext = new TaskContext(KafkaConstants.KAFKA, super.taskContext.getSubProperties(KafkaConstants.KAFKA_));
        this.pollTimeOut = super.taskContext.getLong("polltimeout", 5000L);
        this.checkpoints = new CheckPointSaver<>();
        String ptsStr = super.taskContext.getString("partitions");
        if (StringUtils.isNotBlank(ptsStr)) {
            List<String> pts = Util.splitStringWithFilter(ptsStr, ",", null);
            pts.forEach(s -> partitions.add(new TopicPartition(topic, Integer.valueOf(s))));
        }

    }


    @Override
    public void init() {

        super.init();
    }


    @Override
    public void onStart() {

        super.onStart();
        this.kafkaConsumer = KafkaUtil.createConsumer(this.kafkaContext);
        if (this.partitions.isEmpty()) {
            this.kafkaConsumer.subscribe(Arrays.asList(this.topic), this);
        } else {
            this.kafkaConsumer.assign(partitions);
        }
    }


    @Override
    public void work() {

        ConsumerRecords<byte[], byte[]> records;
        this.kafkaLock.lock();
        try {
            records = this.kafkaConsumer.poll(this.pollTimeOut);
        } finally {
            this.kafkaLock.unlock();
        }
        if (records != null && !records.isEmpty()) {
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
            iterator.forEachRemaining(ele -> {
                KafkaData data = feedOne(ele.key());
                data.setKey(ele.key());
                data.setValue(ele.value());
                data.setPartitionId(ele.partition());
                data.setTimestamp(ele.timestamp());
                push();
            });
            super.handleRows.inc(records.count());
        }
    }


    @Override
    protected void snapshotWhenBarrier(BarrierData barrierData) {

        KafkaCheckPoint checkPoint = new KafkaCheckPoint();
        this.kafkaLock.lock();
        try {
            this.kafkaConsumer.assignment().forEach(topicPartition -> checkPoint.put(topicPartition.partition(), kafkaConsumer
                    .position(new TopicPartition(topic, topicPartition.partition()))));
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


    @Override
    public void completed(BarrierData barrierData, BarrierData coordinatorBarrierData) throws Exception {

        BarrierData barrier = (coordinatorBarrierData != null ? coordinatorBarrierData : barrierData);
        KafkaCheckPoint checkPoint = this.checkpoints.getCheckPoint(barrier.getBarrierId());
        Map<Integer, Long> offsets = checkPoint.getOffsets();
        if (log.isInfoEnabled()) {
            log.info("commit offset : " + JSON.toJSONString(offsets));
        }
        if (offsets.isEmpty()) {
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> params = Maps.newHashMap();
        offsets.forEach((k, v) -> params.put(new TopicPartition(this.topic, k), new OffsetAndMetadata(v)));
        this.kafkaLock.lock();
        try {
            this.kafkaConsumer.commitSync(params);
        } finally {
            this.kafkaLock.unlock();
        }
    }
}
