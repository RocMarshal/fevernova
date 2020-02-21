package com.github.fevernova.task.marketdetail;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.parser.AbstractParser;
import com.github.fevernova.io.kafka.data.KafkaData;
import com.github.fevernova.task.exchange.data.order.OrderAction;
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


    private OrderMatchFactory orderMatchFactory = new OrderMatchFactory();

    private Map<Integer, RateLimiter> limiters = Maps.newHashMap();

    private final int ratio;


    public JobParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum, channelProxy);
        this.ratio = taskContext.getInteger("ratio", 50);
    }


    @Override protected void handleEvent(Data event) {

        KafkaData kafkaData = (KafkaData) event;
        OrderMatch orderMatch = (OrderMatch) this.orderMatchFactory.createData();
        orderMatch.from(kafkaData.getValue());
        if (OrderAction.BID == orderMatch.getOrderAction() && ResultCode.MATCH == orderMatch.getResultCode()) {
            RateLimiter rateLimiter = this.limiters.get(orderMatch.getSymbolId());
            if (rateLimiter == null) {
                rateLimiter = RateLimiter.create(this.ratio);
                this.limiters.put(orderMatch.getSymbolId(), rateLimiter);
            }
            if (rateLimiter.tryAcquire()) {
                OrderDetail orderDetail = feedOne(orderMatch.getSymbolId());
                orderDetail.from(orderMatch);
                push();
            }
        }
    }
}
