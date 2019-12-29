package com.github.fevernova.task.mirrormaker;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.service.barrier.listener.BarrierCompletedListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.io.kafka.AbstractKafkaSource;
import com.github.fevernova.io.kafka.KafkaConstants;
import com.github.fevernova.io.kafka.data.KafkaCheckPoint;
import com.github.fevernova.io.kafka.data.KafkaData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;


@Slf4j
public class JobSource extends AbstractKafkaSource implements BarrierCompletedListener {


    private List<TopicPartition> partitions = Lists.newArrayList();

    private Map<String, String> destTopics;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        super.checkpoints = new CheckPointSaver<>();
        this.destTopics = Maps.newHashMapWithExpectedSize(super.topics.size());
        super.topics.forEach(topic -> {
            TaskContext topicContext = new TaskContext(topic, super.taskContext.getSubProperties(topic + "."));
            String destTopic = topicContext.getString("desttopic");
            this.destTopics.put(topic, destTopic);
            String ptsStr = topicContext.getString(KafkaConstants.PARTITIONS);
            if (StringUtils.isNotBlank(ptsStr)) {
                List<String> pts = Util.splitStringWithFilter(ptsStr, ",", null);
                pts.forEach(s -> partitions.add(new TopicPartition(topic, Integer.valueOf(s))));
            }
        });
    }


    @Override
    public void onStart() {

        super.onStart();
        if (this.partitions.isEmpty()) {
            super.kafkaConsumer.subscribe(super.topics, this);
        } else {
            super.kafkaConsumer.assign(this.partitions);
        }
    }


    @Override protected void kafkaRecords(ConsumerRecords<byte[], byte[]> records) {

        Set<TopicPartition> tmpPartitions = records.partitions();
        for (TopicPartition topicPartition : tmpPartitions) {
            List<ConsumerRecord<byte[], byte[]>> recordList = records.records(topicPartition);
            String destTopic = this.destTopics.get(topicPartition.topic());
            recordList.forEach(ele -> {
                KafkaData data = feedOne(ele.key());
                data.setTopic(ele.topic());
                data.setDestTopic(destTopic);
                data.setKey(ele.key());
                data.setValue(ele.value());
                data.setPartitionId(ele.partition());
                data.setTimestamp(ele.timestamp());
                push();
            });
            super.handleRows.inc(recordList.size());
        }
    }


    @Override
    public void completed(BarrierData barrierData) throws Exception {

        KafkaCheckPoint checkPoint = super.checkpoints.remove(barrierData.getBarrierId());
        Map<TopicPartition, OffsetAndMetadata> params = checkpoint2Params(checkPoint);
        if (!params.isEmpty()) {
            commitKafkaOffset(params);
        }
    }
}
