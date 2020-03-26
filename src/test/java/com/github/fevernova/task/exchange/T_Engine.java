package com.github.fevernova.task.exchange;


import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.cmd.OrderCommandType;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderMode;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.engine.OrderBooksEngine;
import com.github.fevernova.task.exchange.engine.struct.Books;
import org.junit.Assert;


public abstract class T_Engine {


    protected int symbolId = 1;

    protected long userId = 1;

    protected OrderBooksEngine orderBooksEngine;

    protected DataProvider<Integer, OrderMatch> provider;

    protected long orderIdSeq = 1;


    protected OrderCommand buildCMD(OrderCommandType cmdType, OrderAction action, OrderType type, OrderMode mode) {

        OrderCommand cmd = new OrderCommand();
        cmd.setOrderCommandType(cmdType);
        cmd.setOrderId(orderIdSeq++);
        cmd.setSymbolId(symbolId);
        cmd.setUserId(userId);
        cmd.setOrderAction(action);
        cmd.setOrderType(type);
        cmd.setOrderMode(mode);
        return cmd;
    }


    protected void initLastMatchPrice(long price) {

        orderBooksEngine.getSymbols().get(symbolId).setLastMatchPrice(price);
    }


    protected Books getBooks(OrderAction action) {

        return OrderAction.BID == action ? orderBooksEngine.getSymbols().get(symbolId).getBidBooks() :
                orderBooksEngine.getSymbols().get(symbolId).getAskBooks();
    }


    protected void check(int bidNum, int askNum, int matchNum) {

        Assert.assertEquals(bidNum, getBooks(OrderAction.BID).getPriceTree().size());
        Assert.assertEquals(askNum, getBooks(OrderAction.ASK).getPriceTree().size());
        Assert.assertEquals(matchNum, ((TestProvider) provider).getCount());
    }


    protected void parser(OrderCommand orderCommand) {

        if (OrderMode.SIMPLE == orderCommand.getOrderMode()) {
            switch (orderCommand.getOrderCommandType()) {
                case PLACE_ORDER:
                    this.orderBooksEngine.placeOrder(orderCommand, provider);
                    break;
                case CANCEL_ORDER:
                    this.orderBooksEngine.cancelOrder(orderCommand, provider);
                    break;
                case HEARTBEAT:
                    this.orderBooksEngine.heartBeat(orderCommand, provider);
                    break;
            }
        } else {
            switch (orderCommand.getOrderCommandType()) {
                case PLACE_ORDER:
                    this.orderBooksEngine.placeConditionOrder(orderCommand, provider);
                    break;
                case CANCEL_ORDER:
                    this.orderBooksEngine.cancelConditionOrder(orderCommand, provider);
                    break;
            }
        }
    }

}
