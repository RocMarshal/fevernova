package com.github.fevernova.task.mirrormaker;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.io.kafka.AbstractKafkaSink;
import com.github.fevernova.io.kafka.data.KafkaData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;


@Slf4j
public class JobSink extends AbstractKafkaSink {


    private boolean partitionRebalance;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.partitionRebalance = taskContext.getBoolean("partitionrebalance", false);
    }


    @Override
    protected void handleEvent(Data event) {

        KafkaData data = (KafkaData) event;
        if (data.getValue() == null) {
            return;
        }
        Integer pt = (this.partitionRebalance || data.getPartitionId() < 0 ? null : (data.getPartitionId()));
        String targetTopic = (data.getDestTopic() != null ? data.getDestTopic() : super.topic);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(targetTopic, pt, data.getTimestamp(), data.getKey(), data.getValue());
        super.kafka.send(record, this);
    }
}
