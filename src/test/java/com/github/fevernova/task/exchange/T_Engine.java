package com.github.fevernova.task.exchange;


import com.github.fevernova.Common;
import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.ComponentType;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.framework.service.state.BinaryFileIdentity;
import com.github.fevernova.framework.service.state.storage.FSStorage;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.OrderMatchFactory;
import com.github.fevernova.task.exchange.engine.OrderBooksEngine;
import org.apache.commons.lang3.Validate;
import org.junit.Before;
import org.junit.Test;


public class T_Engine {


    private OrderBooksEngine orderBooksEngine;

    private FSStorage fsStorage;

    private BinaryFileIdentity identity;

    private DataProvider<Integer, OrderMatch> provider;


    @Before
    public void init() {

        GlobalContext globalContext = Common.createGlobalContext();
        TaskContext taskContext = Common.createTaskContext();
        orderBooksEngine = new OrderBooksEngine(globalContext, taskContext);
        fsStorage = new FSStorage(globalContext, taskContext);
        identity = BinaryFileIdentity.builder().componentType(ComponentType.PARSER).total(3).index(1).identity(OrderBooksEngine.CONS_NAME).build();
        provider = new DataProvider<Integer, OrderMatch>() {


            private OrderMatch orderMatch = (OrderMatch) new OrderMatchFactory().createData();

            private boolean flag = false;


            @Override public OrderMatch feedOne(Integer key) {

                Validate.isTrue(!flag);
                flag = true;
                return orderMatch;
            }


            @Override public void push() {

                Validate.isTrue(flag);
                flag = false;
                orderMatch.clearData();
            }
        };
    }


    private int parser(OrderCommand orderCommand) {

        if (OrderCommandType.PLACE_ORDER == orderCommand.getOrderCommandType()) {
            orderBooksEngine.placeOrder(orderCommand, provider, true);
        } else if (OrderCommandType.CANCEL_ORDER == orderCommand.getOrderCommandType()) {
            orderBooksEngine.cancelOrder(orderCommand, provider);
        }
        return 1;
    }


    @Test
    public void T_snapshot() {

        OrderCommand bidCMD = new OrderCommand();
        bidCMD.setOrderCommandType(OrderCommandType.PLACE_ORDER);
        bidCMD.setOrderId(1);
        bidCMD.setSymbolId(1);
        bidCMD.setUserId(1);
        bidCMD.setTimestamp(Util.nowMS());
        bidCMD.setOrderAction(OrderAction.BID);
        bidCMD.setOrderType(OrderType.GTC);
        bidCMD.setPrice(1000000);
        bidCMD.setSize(100);

        OrderCommand askCMD = new OrderCommand();
        askCMD.setOrderCommandType(OrderCommandType.PLACE_ORDER);
        askCMD.setOrderId(2);
        askCMD.setSymbolId(1);
        askCMD.setUserId(1);
        askCMD.setTimestamp(Util.nowMS());
        askCMD.setOrderAction(OrderAction.ASK);
        askCMD.setOrderType(OrderType.GTC);
        askCMD.setPrice(10000000);
        askCMD.setSize(100);

        for (int i = 0; i < 100000; i++) {
            bidCMD.setOrderId(bidCMD.getOrderId() + 2);
            askCMD.setOrderId(askCMD.getOrderId() + 2);
            bidCMD.setPrice(bidCMD.getPrice() - 1);
            askCMD.setPrice(askCMD.getPrice() + 1);
            parser(bidCMD);
            parser(askCMD);
        }

        fsStorage.saveBinary(identity, new BarrierData(1L, 0L), orderBooksEngine);
    }


    @Test
    public void T_load() {

        OrderBooksEngine orderBooksEngine = new OrderBooksEngine(null, null);
        fsStorage.recoveryBinary("/tmp/fevernova/testtype-testid/3-0/data/parser_3_1_OrderBooksEngine_0_1.bin", orderBooksEngine);
        System.out.println(orderBooksEngine);

    }


    @Test
    public void T_parser() {

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
        askCMD.setOrderId(2);
        askCMD.setSymbolId(1);
        askCMD.setUserId(1);
        askCMD.setTimestamp(Util.nowMS());
        askCMD.setOrderAction(OrderAction.ASK);
        askCMD.setOrderType(OrderType.GTC);
        askCMD.setPrice(10);
        askCMD.setSize(100);

        Common.warmup();

        long st = Util.nowMS();
        int bidSum = 0, askSum = 0;
        for (int i = 0; i < 1_0000; i++) {
            for (int m = 1; m < 101; m++) {
                bidCMD.setPrice(m);
                askCMD.setPrice(101 - m);
                for (int x = 0; x < 100; x++) {
                    bidSum += parser(bidCMD);
                    bidCMD.setOrderId(bidCMD.getOrderId() + 2);
                    askSum += parser(askCMD);
                    askCMD.setOrderId(askCMD.getOrderId() + 2);
                }
            }
        }
        long et = Util.nowMS();
        System.out.println(et - st);
        System.out.println(bidSum + "-" + askSum);
    }
}
