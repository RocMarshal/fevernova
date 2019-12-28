package com.github.fevernova.task.mirrormaker;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractSink;
import com.github.fevernova.io.kafka.KafkaConstants;
import com.github.fevernova.io.kafka.KafkaUtil;
import com.github.fevernova.io.kafka.data.KafkaData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class JobSink extends AbstractSink implements Callback {


    private TaskContext kafkaContext;

    private KafkaProducer<byte[], byte[]> kafka;

    private String destTopic;

    private AtomicInteger errorCounter;

    private boolean partitionRebalance;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.kafkaContext = new TaskContext(KafkaConstants.KAFKA, taskContext.getSubProperties(KafkaConstants.KAFKA_));
        this.destTopic = taskContext.getString(KafkaConstants.TOPIC);
        this.partitionRebalance = taskContext.getBoolean("partitionrebalance", false);
        this.errorCounter = new AtomicInteger(0);
    }


    @Override
    public void onStart() {

        super.onStart();
        this.kafka = KafkaUtil.createProducer(this.kafkaContext);
    }


    @Override
    protected void handleEvent(Data event) {

        KafkaData data = (KafkaData) event;
        if (data.getValue() == null) {
            return;
        }
        Integer pt = (this.partitionRebalance ? null : data.getPartitionId());
        String targetTopic = (data.getDestTopic() != null ? data.getDestTopic() : this.destTopic);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(targetTopic, pt, data.getTimestamp(), data.getKey(), data.getValue());
        this.kafka.send(record, this);
    }


    @Override
    protected void timeOut() {

        flush();
    }


    @Override
    protected void snapshotWhenBarrier(BarrierData barrierData) {

        flush();
    }


    private void flush() {

        this.kafka.flush();
        if (this.errorCounter.get() > 0) {
            super.globalContext.fatalError("flush kafka error");
        }
    }


    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {

        if (exception != null) {
            log.error("Kafka Error : ", exception);
            this.errorCounter.incrementAndGet();
        }
    }

}
