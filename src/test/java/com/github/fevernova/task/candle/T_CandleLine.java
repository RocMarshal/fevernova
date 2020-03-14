package com.github.fevernova.task.candle;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.OrderMatchFactory;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.marketcandle.data.CandleData;
import com.github.fevernova.task.marketcandle.data.INotify;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;


@Slf4j
public class T_CandleLine {


    private CandleData candleData = new CandleData();

    private INotify notify = (symbolId, points) -> System.out.println(Arrays.toString(points.toArray()));

    private OrderMatch orderMatch = (OrderMatch) new OrderMatchFactory().createData();


    @Before
    public void init() {

        this.orderMatch.setSequence(1L);
        this.orderMatch.setSymbolId(1);
        this.orderMatch.setOrderId(1L);
        this.orderMatch.setUserId(1L);
        this.orderMatch.setTimestamp(1577808000000L);
        this.orderMatch.setOrderAction(OrderAction.BID);
        this.orderMatch.setOrderType(OrderType.GTC);
        this.orderMatch.setOrderPrice(1L);
        this.orderMatch.setOrderPriceDepthSize(1L);
        this.orderMatch.setOrderPriceOrderCount(1);
        this.orderMatch.setOrderTotalSize(1L);
        this.orderMatch.setOrderAccFilledSize(1L);
        this.orderMatch.setOrderVersion(1);
        this.orderMatch.setMatchPrice(1L);
        this.orderMatch.setMatchSize(1L);
        this.orderMatch.setMatchOrderId(1L);
        this.orderMatch.setMatchOrderUserId(1L);
        this.orderMatch.setDriverAction(OrderAction.ASK);
        this.orderMatch.setResultCode(ResultCode.MATCH);
    }


    @Test
    public void T_CandleLIne() {

        while (true) {
            this.orderMatch.setSequence(this.orderMatch.getSequence() + 1);
            this.orderMatch.setTimestamp(this.orderMatch.getTimestamp() + 1000);
            this.orderMatch.setMatchPrice(this.orderMatch.getMatchPrice() + 1);
            this.candleData.handle(this.orderMatch, this.notify);
            Util.sleepMS(100);
        }
    }
}
