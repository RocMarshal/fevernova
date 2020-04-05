package com.github.fevernova.task.binlog;


import com.github.fevernova.framework.common.LogProxy;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.io.data.message.SerializerHelper;
import com.github.fevernova.io.kafka.AbstractKafkaSink;
import com.github.fevernova.task.binlog.data.MessageData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;


@Slf4j
public class JobSink extends AbstractKafkaSink {


    private SerializerHelper serializerHelper;

    private boolean convert2json;


    public JobSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.convert2json = taskContext.getBoolean("json", false);
        this.serializerHelper = new SerializerHelper();
    }


    @Override
    public void onStart() {

        super.onStart();
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
        String targetTopic = (data.getDestTopic() != null ? data.getDestTopic() : super.topic);
        byte[] value;
        if (this.convert2json) {
            value = this.serializerHelper.serializeJSON(null, data.getDataContainer());
            if (LogProxy.LOG_DATA.isTraceEnabled()) {
                LogProxy.LOG_DATA.trace(new String(value));
            }
        } else {
            value = this.serializerHelper.serialize(null, data.getDataContainer());
        }

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(targetTopic, null, data.getTimestamp(), data.getKey(), value);
        super.kafka.send(record, this);
    }

}
