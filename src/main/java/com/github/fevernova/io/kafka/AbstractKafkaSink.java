package com.github.fevernova.io.kafka;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.sink.AbstractSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public abstract class AbstractKafkaSink extends AbstractSink implements Callback {


    private TaskContext kafkaContext;

    protected KafkaProducer<byte[], byte[]> kafka;

    protected String topic;

    private AtomicInteger errorCounter;


    public AbstractKafkaSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.kafkaContext = new TaskContext(KafkaConstants.KAFKA, taskContext.getSubProperties(KafkaConstants.KAFKA_));
        this.topic = taskContext.getString("topic");
        this.errorCounter = new AtomicInteger(0);
    }


    @Override
    public void onStart() {

        super.onStart();
        this.kafka = KafkaUtil.createProducer(this.kafkaContext);
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
            log.error("Kafka Error :", exception);
            this.errorCounter.incrementAndGet();
        }
    }
}
