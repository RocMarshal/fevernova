package com.github.fevernova.task.exchange;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.data.message.DataContainer;
import com.github.fevernova.data.message.SerializerHelper;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.framework.service.checkpoint.ICheckPointSaver;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.kafka.KafkaConstants;
import com.github.fevernova.kafka.KafkaUtil;
import com.github.fevernova.kafka.data.KafkaCheckPoint;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;


@Slf4j
public class JobSource extends AbstractSource<byte[], OrderCommand> implements ConsumerRebalanceListener, BarrierCoordinatorListener {


    protected ICheckPointSaver<KafkaCheckPoint> checkpoints;

    private TaskContext kafkaContext;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    private ReentrantLock kafkaLock = new ReentrantLock();

    private String topic;

    private long pollTimeOut;

    private SerializerHelper serializer = new SerializerHelper();


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.topic = super.taskContext.get(KafkaConstants.TOPICS);
        this.kafkaContext = new TaskContext(KafkaConstants.KAFKA, super.taskContext.getSubProperties(KafkaConstants.KAFKA_));
        this.pollTimeOut = super.taskContext.getLong(KafkaConstants.POLLTIMEOUT, 5000L);
        this.checkpoints = new CheckPointSaver<>();
    }


    @Override public void init() {

        super.init();
        this.kafkaConsumer = KafkaUtil.createConsumer(this.kafkaContext);
    }


    @Override
    public void onStart() {

        super.onStart();
        this.kafkaConsumer.subscribe(Lists.newArrayList(this.topic), this);
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
            Set<TopicPartition> tmpPartitions = records.partitions();
            for (TopicPartition topicPartition : tmpPartitions) {
                List<ConsumerRecord<byte[], byte[]>> recordList = records.records(topicPartition);
                recordList.forEach(ele -> {
                    DataContainer dataContainer = this.serializer.deserialize(null, ele.value());
                    final OrderCommand data = feedOne(ele.key());
                    dataContainer.iterate((metaEntity, change, val, oldVal) -> {

                        switch (metaEntity.getColumnName()) {
                            case "orderCommandType":
                                data.setOrderCommandType(OrderCommandType.of((int) val));
                                break;
                            case "orderId":
                                data.setOrderId((long) val);
                                break;
                            case "symbolId":
                                data.setSymbolId((int) val);
                                break;
                            case "userId":
                                data.setUserId((long) val);
                                break;
                            case "timestamp":
                                data.setTimestamp((long) val);
                                break;
                            case "action":
                                data.setOrderAction(OrderAction.of((int) val));
                                break;
                            case "orderType":
                                data.setOrderType(OrderType.of((int) val));
                                break;
                            case "price":
                                data.setPrice((long) val);
                                break;
                            case "size":
                                data.setSize((long) val);
                                break;
                        }
                    });
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
        stateValue.setValue(checkPoint);
        return stateValue;
    }


    @Override public void onRecovery(List<StateValue> stateValues) {

        super.onRecovery(stateValues);
        for (StateValue stateValue : stateValues) {
            KafkaCheckPoint kafkaCheckPoint = new KafkaCheckPoint();
            kafkaCheckPoint.parseFromJSON((JSONObject) stateValue.getValue());
            Map<TopicPartition, OffsetAndMetadata> params = Maps.newHashMap();
            kafkaCheckPoint.getOffsets()
                    .forEach((topic, offset) -> offset.forEach((k, v) -> params.put(new TopicPartition(topic, k), new OffsetAndMetadata(v))));
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
        } finally {
            this.kafkaLock.unlock();
        }
    }
}
