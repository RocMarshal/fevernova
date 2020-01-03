package com.github.fevernova.task.test2kafka;


import com.github.fevernova.framework.common.LogProxy;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.structure.rb.IRingBuffer;
import com.github.fevernova.framework.common.structure.rb.SimpleRingBuffer;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.source.AbstractSource;
import com.github.fevernova.io.data.type.impl.UInteger;
import com.github.fevernova.io.kafka.data.KafkaData;
import com.github.fevernova.task.exchange.Confrontation;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;


@Slf4j
public class JobSource extends AbstractSource<Integer, KafkaData> {


    private Confrontation confrontation;

    private IRingBuffer<OrderCommand> iRingBuffer = new SimpleRingBuffer<>(128);

    final UInteger uInteger = new UInteger(false);


    public JobSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.confrontation = new Confrontation(this.iRingBuffer);
    }


    @Override public void onStart() {

        super.onStart();
        new Thread(this.confrontation).start();

    }


    @Override public void work() {

        Optional<OrderCommand> optional = this.iRingBuffer.get();
        if (optional == null) {
            Util.sleepMS(1);
            waitTime(1_000_000L);
            return;
        }

        if (LogProxy.LOG_DATA.isDebugEnabled()) {
            LogProxy.LOG_DATA.debug(optional.get().toString());
        }

        OrderCommand orderCommand = optional.get();
        KafkaData kafkaData = feedOne(orderCommand.getSymbolId());
        uInteger.from(orderCommand.getSymbolId());
        kafkaData.setKey(uInteger.toBytes());
        kafkaData.setValue(orderCommand.to());
        kafkaData.setTimestamp(orderCommand.getTimestamp());
    }
}
