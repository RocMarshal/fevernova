package com.github.fevernova.task.exchangetestdata;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.structure.rb.IRingBuffer;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.OrderMatchFactory;
import com.github.fevernova.task.exchange.engine.OrderBooks;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


public class Confrontation implements Runnable {


    static long orderIdSeq = 1L;

    static int symbolId = 999;

    static long userId = 999999999L;

    static Random random = new Random();

    final OrderBooks orderBooks = new OrderBooks(symbolId);

    long middilePrice = 1_0000_0000L;

    long maxFloat = 10L;

    long baseDepth;

    long loopTimes;

    final AtomicLong resultCount = new AtomicLong(0L);

    final DataProvider<Long, OrderMatch> dataProvider = new DataProvider<Long, OrderMatch>() {


        private OrderMatch orderMatch = (OrderMatch) new OrderMatchFactory().createData();


        @Override public OrderMatch feedOne(Long key) {

            resultCount.incrementAndGet();
            return this.orderMatch;
        }


        @Override public void push() {

            this.orderMatch.clearData();
        }
    };

    private IRingBuffer<OrderCommand> iRingBuffer;

    private TaskContext taskContext;


    public Confrontation(TaskContext taskContext, IRingBuffer<OrderCommand> iRingBuffer) {

        this.taskContext = taskContext;
        this.iRingBuffer = iRingBuffer;
        this.baseDepth = taskContext.getLong("basedepth", 1000L);
        this.loopTimes = taskContext.getLong("looptimes", 1_0000_0000L);
    }


    @Override
    public void run() {
        //买的深度
        for (long i = (middilePrice - maxFloat - baseDepth); i < (middilePrice - maxFloat); i++) {
            OrderCommand cmd = build(OrderAction.BID, i, 100000000L);
            orderBooks.place(cmd, dataProvider);
        }
        //卖的深度
        for (long i = (middilePrice + maxFloat); i < (middilePrice + maxFloat + baseDepth); i++) {
            OrderCommand cmd = build(OrderAction.ASK, i, 100000000L);
            orderBooks.place(cmd, dataProvider);
        }

        orderBooks.setLastMatchPrice(middilePrice);

        while (loopTimes-- > 0) {
            long bidPrice = orderBooks.getBidBooks().getPrice() > (middilePrice - maxFloat) ? orderBooks.getBidBooks().getPrice() + 1 : middilePrice;
            long bidSize = (long) random.nextInt(500);
            OrderCommand bidCmd = build(OrderAction.BID, bidPrice, bidSize);
            orderBooks.place(bidCmd, dataProvider);

            long askPrice = orderBooks.getAskBooks().getPrice() < (middilePrice + maxFloat) ? orderBooks.getAskBooks().getPrice() - 1 : middilePrice;
            long askSize = (long) random.nextInt(500);
            OrderCommand askCmd = build(OrderAction.ASK, askPrice, askSize);
            orderBooks.place(askCmd, dataProvider);
        }
    }


    public OrderCommand build(OrderAction orderAction, long price, long size) {

        OrderCommand orderCommand = new OrderCommand();
        orderCommand.setOrderCommandType(OrderCommandType.PLACE_ORDER);
        orderCommand.setOrderId(orderIdSeq++);
        orderCommand.setSymbolId(symbolId);
        orderCommand.setUserId(userId);
        orderCommand.setTimestamp(Util.nowMS());
        orderCommand.setOrderAction(orderAction);
        orderCommand.setOrderType(OrderType.GTC);
        orderCommand.setPrice(price);
        orderCommand.setSize(size);

        while (!iRingBuffer.add(orderCommand)) {}
        return orderCommand;
    }

}
