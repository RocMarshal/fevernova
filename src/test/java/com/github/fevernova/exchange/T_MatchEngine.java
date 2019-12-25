package com.github.fevernova.exchange;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.engine.OrderBooksEngine;
import com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import java.util.TreeMap;


public class T_MatchEngine {


    private OrderBooksEngine orderBooksEngine;


    @Before
    public void init() {

        this.orderBooksEngine = new OrderBooksEngine(null, null);
    }


    @Test
    public void T_placeOrder() {

        OrderCommand bidCMD = new OrderCommand();
        bidCMD.setOrderCommandType(OrderCommandType.PLACE_ORDER);
        bidCMD.setOrderId(1);
        bidCMD.setSymbolId(1);
        bidCMD.setUserId(1);
        bidCMD.setTimestamp(Util.nowMS());
        bidCMD.setOrderAction(OrderAction.BID);
        bidCMD.setOrderType(OrderType.GTC);
        bidCMD.setPrice(10);
        bidCMD.setSize(100);

        OrderCommand askCMD = new OrderCommand();
        askCMD.setOrderCommandType(OrderCommandType.PLACE_ORDER);
        askCMD.setOrderId(1);
        askCMD.setSymbolId(1);
        askCMD.setUserId(1);
        askCMD.setTimestamp(Util.nowMS());
        askCMD.setOrderAction(OrderAction.ASK);
        askCMD.setOrderType(OrderType.GTC);
        askCMD.setPrice(8);
        askCMD.setSize(100);

        long base = 9000000000L;

        long k = 0, j = 0;
        while (k++ < base) {
            j += k;
        }
        System.out.println(j);

        long st = Util.nowMS();
        int bidSum = 0, askSum = 0;
        for (int i = 0; i < 1_0000; i++) {
            for (int m = 1; m < 101; m++) {
                bidCMD.setPrice(m);
                for (int x = 0; x < 100; x++)
                    bidSum += this.orderBooksEngine.placeOrder(bidCMD).size();
            }
            for (int n = 100; n > 0; n--) {
                askCMD.setPrice(n);
                for (int x = 0; x < 100; x++)
                    askSum += this.orderBooksEngine.placeOrder(askCMD).size();
            }
        }
        long et = Util.nowMS();
        System.out.println(et - st);
        System.out.println(bidSum + "-" + askSum);
    }


    @Test
    public void T_treeMap() {

        long base = 9000000000L;

        long k = 0, j = 0;
        while (k++ < base) {
            j += k;
        }
        System.out.println(j);


        TreeMap<Long, String> map = Maps.newTreeMap(Long::compareTo);
        for (long i = 0; i < 100; i++) {
            map.put(i, i + "");
        }
        long x = 0;
        long st = Util.nowMS();
        for (long i = 0; i < 1000000000; i++) {
            x += map.firstEntry().getValue().length();
            //            x += map.get(0L).length();
        }
        long et = Util.nowMS();
        System.out.println(et - st);
        System.out.println(x);

    }

}
