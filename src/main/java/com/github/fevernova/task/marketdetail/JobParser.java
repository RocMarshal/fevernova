package com.github.fevernova.task.marketdetail;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.io.kafka.data.KafkaData;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.OrderMatchFactory;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.marketdetail.data.OrderDetail;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;


@Slf4j
public class JobParser extends AbstractParser<Integer, OrderDetail> {


    private final int ratio;

    private OrderMatch orderMatch = (OrderMatch) new OrderMatchFactory().createData();

    private Map<Integer, RateLimiter> limiters = Maps.newHashMap();


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.ratio = taskContext.getInteger("ratio", 50);
    }


    @Override protected void handleEvent(Data event) {

        KafkaData kafkaData = (KafkaData) event;
        this.orderMatch.from(kafkaData.getValue());
        if (ResultCode.MATCH == this.orderMatch.getResultCode()) {
            RateLimiter rateLimiter = this.limiters.get(this.orderMatch.getSymbolId());
            if (rateLimiter == null) {
                rateLimiter = RateLimiter.create(this.ratio);
                this.limiters.put(this.orderMatch.getSymbolId(), rateLimiter);
            }
            if (rateLimiter.tryAcquire()) {
                OrderDetail orderDetail = feedOne(this.orderMatch.getSymbolId());
                orderDetail.from(this.orderMatch);
                push();
            }
        }
    }
}
