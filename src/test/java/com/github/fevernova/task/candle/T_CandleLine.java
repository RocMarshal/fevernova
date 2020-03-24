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

        this.orderMatch.setSymbolId(1);
        this.orderMatch.setTimestamp(1577808000000L);
        this.orderMatch.getMatchPart().setMatchPrice(1L);
        this.orderMatch.getMatchPart().setMatchSize(1L);
        this.orderMatch.getMatchPart().setDriverAction(OrderAction.ASK);
        this.orderMatch.setResultCode(ResultCode.MATCH);
        this.orderMatch.getOrderPart1().setSequence(1L);
        this.orderMatch.getOrderPart1().setOrderId(1L);
        this.orderMatch.getOrderPart1().setUserId(1L);
        this.orderMatch.getOrderPart1().setOrderAction(OrderAction.BID);
        this.orderMatch.getOrderPart1().setOrderType(OrderType.GTC);
        this.orderMatch.getOrderPart1().setOrderPrice(1L);
        this.orderMatch.getOrderPart1().setOrderPriceDepthSize(1L);
        this.orderMatch.getOrderPart1().setOrderPriceOrderCount(1);
        this.orderMatch.getOrderPart1().setOrderTotalSize(1L);
        this.orderMatch.getOrderPart1().setOrderAccFilledSize(1L);
        this.orderMatch.getOrderPart1().setOrderVersion(1);
    }


    @Test
    public void T_CandleLIne() {

        this.orderMatch.getOrderPart1().setSequence(this.orderMatch.getOrderPart1().getSequence() + 1);
        this.orderMatch.setTimestamp(Util.nowMS());
        this.orderMatch.getMatchPart().setMatchPrice(this.orderMatch.getMatchPart().getMatchPrice() + 1);
        this.candleData.handle(this.orderMatch, this.notify);
        while (true) {
            this.orderMatch.setTimestamp(this.orderMatch.getTimestamp());
            this.candleData.scan4Update(true, this.notify, Util.nowMS());
            Util.sleepMS(100);
        }
    }
}
