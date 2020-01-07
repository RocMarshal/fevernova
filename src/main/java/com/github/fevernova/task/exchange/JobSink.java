package com.github.fevernova.task.exchange;


import com.github.fevernova.framework.common.LogProxy;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractSink;
import com.github.fevernova.io.data.type.impl.ULong;
import com.github.fevernova.io.kafka.KafkaConstants;
import com.github.fevernova.io.kafka.KafkaUtil;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
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

    private String topic;

    private AtomicInteger errorCounter;

    private ULong uLong = new ULong(false);


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.kafkaContext = new TaskContext(KafkaConstants.KAFKA, taskContext.getSubProperties(KafkaConstants.KAFKA_));
        this.topic = taskContext.getString(KafkaConstants.TOPIC);
        this.errorCounter = new AtomicInteger(0);
    }


    @Override
    public void onStart() {

        super.onStart();
        this.kafka = KafkaUtil.createProducer(this.kafkaContext);
    }


    @Override
    protected void handleEvent(Data event) {

        OrderMatch data = (OrderMatch) event;
        if (LogProxy.LOG_DATA.isTraceEnabled()) {
            LogProxy.LOG_DATA.trace(data.toString());
        }

        this.uLong.from(data.getOrderId());
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(this.topic, null, Util.nowMS(), this.uLong.toBytes(), data.to());
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
            log.error("Kafka Error :", exception);
            this.errorCounter.incrementAndGet();
        }
    }

}
