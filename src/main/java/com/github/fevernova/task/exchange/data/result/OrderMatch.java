package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@Setter
@ToString
public class OrderMatch implements Data {


    private long orderId;

    private int symbolId;

    private long userId;

    private long timestamp;

    private OrderAction orderAction;

    private OrderType orderType;

    private long price;

    private long totalSize;

    private long accFilledSize;

    private long matchFilledSize;

    private long matchOrderId;

    private long matchOrderUserId;

    private int version;

    private ResultCode resultCode;


    //    public void from(OrderMatch orderMatch) {
    //
    //        this.orderId = orderMatch.getOrderId();
    //        this.symbolId = orderMatch.getSymbolId();
    //        this.userId = orderMatch.getUserId();
    //        this.timestamp = orderMatch.getTimestamp();
    //        this.orderAction = orderMatch.getOrderAction();
    //        this.orderType = orderMatch.getOrderType();
    //        this.price = orderMatch.getPrice();
    //        this.totalSize = orderMatch.getTotalSize();
    //        this.accFilledSize = orderMatch.getAccFilledSize();
    //        this.matchFilledSize = orderMatch.getMatchFilledSize();
    //        this.matchOrderId = orderMatch.getMatchOrderId();
    //        this.matchOrderUserId = orderMatch.getMatchOrderUserId();
    //        this.version = orderMatch.getVersion();
    //        this.resultCode = orderMatch.getResultCode();
    //    }


    public void from(OrderCommand orderCommand) {

        this.orderId = orderCommand.getOrderId();
        this.symbolId = orderCommand.getSymbolId();
        this.userId = orderCommand.getUserId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderAction = orderCommand.getOrderAction();
        this.orderType = orderCommand.getOrderType();
        this.price = orderCommand.getPrice();
        this.totalSize = orderCommand.getSize();
        //this.accFilledSize = 0L;
        //this.matchFilledSize = 0L;
        //this.matchOrderId = 0L;
        //this.matchOrderUserId = 0L;
        //this.version = 0;
    }


    public void from(OrderCommand orderCommand, Order order) {

        this.orderId = orderCommand.getOrderId();
        this.symbolId = orderCommand.getSymbolId();
        this.userId = orderCommand.getUserId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderAction = orderCommand.getOrderAction();
        this.orderType = orderCommand.getOrderType();
        this.price = orderCommand.getPrice();
        this.totalSize = orderCommand.getSize();
        this.accFilledSize = order.getFilledSize();
        //this.matchFilledSize = 0L;
        //this.matchOrderId = 0L;
        //this.matchOrderUserId = 0L;
        this.version = order.getVersion();
        this.resultCode = ResultCode.CANCEL_IOC;
    }


    public void from(Order order, int symbolId, OrderAction orderAction, long price, long matchFilledSize, Order thatOrder) {

        this.orderId = order.getOrderId();
        this.symbolId = symbolId;
        this.userId = order.getUserId();
        this.timestamp = Util.nowMS();
        this.orderAction = orderAction;
        this.orderType = order.getOrderType();
        this.price = price;
        this.totalSize = order.getRemainSize() + order.getFilledSize();
        this.accFilledSize = order.getFilledSize();
        this.matchFilledSize = matchFilledSize;
        this.matchOrderId = thatOrder.getOrderId();
        this.matchOrderUserId = thatOrder.getUserId();
        this.version = order.getVersion();
        this.resultCode = ResultCode.MATCH;
    }


    @Override public void clearData() {

        this.orderId = 0L;
        this.symbolId = 0;
        this.userId = 0L;
        this.timestamp = 0L;
        this.orderAction = null;
        this.orderType = null;
        this.price = 0L;
        this.totalSize = 0L;
        this.accFilledSize = 0L;
        this.matchFilledSize = 0L;
        this.matchOrderId = 0L;
        this.matchOrderUserId = 0L;
        this.version = 0;
        this.resultCode = null;
    }


    @Override public byte[] getBytes() {

        return new byte[0];
    }
}
