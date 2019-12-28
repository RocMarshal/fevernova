package com.github.fevernova.task.performance;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.io.kafka.data.KafkaData;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class JobParser extends AbstractParser<Integer, KafkaData> {


    boolean forward;


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.forward = taskContext.getBoolean("forward", false);
    }


    @Override protected void handleEvent(Data event) {

        KafkaData kafkaData = (KafkaData) event;

        if (log.isTraceEnabled()) {
            log.trace(new String(kafkaData.getValue()));
        }

        if (this.forward) {
            KafkaData container = feedOne(kafkaData.getPartitionId());
            container.setKey(kafkaData.getKey());
            container.setValue(kafkaData.getValue());
            container.setPartitionId(kafkaData.getPartitionId());
            container.setTimestamp(kafkaData.getTimestamp());
            push();
        }
    }

}
