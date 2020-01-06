package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.LogProxy;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractSink;
import com.github.fevernova.io.data.message.SerializerHelper;
import com.github.fevernova.io.kafka.KafkaConstants;
import com.github.fevernova.io.kafka.KafkaUtil;
import com.github.fevernova.task.binlog.data.MessageData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class JobSink extends AbstractSink implements Callback {


    private final boolean test;

    private TaskContext kafkaContext;

    private KafkaProducer<byte[], byte[]> kafka;

    private String defaultTopic;

    private AtomicInteger errorCounter;

    private SerializerHelper serializerHelper;

    private boolean convert2json;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.kafkaContext = new TaskContext(KafkaConstants.KAFKA, taskContext.getSubProperties(KafkaConstants.KAFKA_));
        this.defaultTopic = taskContext.getString("defaulttopic");
        this.convert2json = taskContext.getBoolean("json", false);
        this.test = taskContext.getBoolean("test", false);
        this.errorCounter = new AtomicInteger(0);
        this.serializerHelper = new SerializerHelper();
    }


    @Override
    public void onStart() {

        super.onStart();
        if (!this.test) {
            this.kafka = KafkaUtil.createProducer(this.kafkaContext);
        }
    }


    @Override
    protected void handleEvent(Data event) {

        MessageData data = (MessageData) event;
        if (data.getDataContainer() == null) {
            return;
        }
        if (LogProxy.LOG_DATA.isTraceEnabled()) {
            LogProxy.LOG_DATA.trace(data.getDataContainer().getData().toString());
        }
        String targetTopic = (data.getDestTopic() != null ? data.getDestTopic() : this.defaultTopic);
        byte[] value;
        if (this.convert2json) {
            value = this.serializerHelper.serializeJSON(null, data.getDataContainer());
            if (LogProxy.LOG_DATA.isTraceEnabled()) {
                LogProxy.LOG_DATA.trace(new String(value));
            }
        } else {
            value = this.serializerHelper.serialize(null, data.getDataContainer());
        }
        if (!this.test) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(targetTopic, null, data.getTimestamp(), data.getKey(), value);
            this.kafka.send(record, this);
        }
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

        if (!this.test) {
            this.kafka.flush();
        }
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
