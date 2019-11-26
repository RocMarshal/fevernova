package com.github.fevernova.task.mirrormaker;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.kafka.data.KafkaData;
import lombok.extern.slf4j.Slf4j;


@Slf4j(topic = "fevernova-data")
public class JobParser extends AbstractParser<byte[], KafkaData> {


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
    }


    @Override
    protected void handleEvent(Data event) {

        KafkaData kafkaData = (KafkaData) event;

        if (log.isTraceEnabled()) {
            log.trace(new String(kafkaData.getValue()));
        }

        KafkaData container = feedOne(kafkaData.getKey());
        container.setKey(kafkaData.getKey());
        container.setValue(kafkaData.getValue());
        container.setPartitionId(kafkaData.getPartitionId());
        container.setTimestamp(kafkaData.getTimestamp());
        push();
    }


    @Override
    protected BroadcastData onBroadcast(BroadcastData broadcastData) {

        return broadcastData;
    }

}
