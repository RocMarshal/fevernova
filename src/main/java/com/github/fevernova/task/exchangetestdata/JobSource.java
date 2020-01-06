package com.github.fevernova.task.exchangetestdata;


import com.github.fevernova.framework.common.LogProxy;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.structure.rb.IRingBuffer;
import com.github.fevernova.framework.common.structure.rb.SimpleRingBuffer;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.io.data.type.impl.UInteger;
import com.github.fevernova.io.kafka.data.KafkaData;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;


@Slf4j
public class JobSource extends AbstractSource<Integer, KafkaData> {


    private final IRingBuffer<OrderCommand> ringBuffer = new SimpleRingBuffer<>(128);

    private final UInteger uInteger = new UInteger(false);

    private final Confrontation confrontation;


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.confrontation = new Confrontation(taskContext, this.ringBuffer);
    }


    @Override public void onStart() {

        super.onStart();
        new Thread(this.confrontation).start();
    }


    @Override public void work() {

        Optional<OrderCommand> optional = this.ringBuffer.get();
        if (!optional.isPresent()) {
            return;
        }

        if (LogProxy.LOG_DATA.isTraceEnabled()) {
            LogProxy.LOG_DATA.trace(optional.get().toString());
        }

        OrderCommand orderCommand = optional.get();
        KafkaData kafkaData = feedOne(orderCommand.getSymbolId());
        this.uInteger.from(orderCommand.getSymbolId());
        kafkaData.setKey(this.uInteger.toBytes());
        kafkaData.setValue(orderCommand.to());
        kafkaData.setTimestamp(orderCommand.getTimestamp());
        push();
    }
}
