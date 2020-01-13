package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.task.exchange.data.Sequence;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.nio.ByteBuffer;


@Getter
@Setter
@ToString
public class OrderMatch implements Data {


    private long sequence;

    private long orderId;

    private int symbolId;

    private long userId;

    private long timestamp;

    private OrderAction orderAction;

    private OrderType orderType;

    private long orderPrice;

    private long matchPrice;

    private long depthSize;

    private long totalSize;

    private long accFilledSize;

    private long matchFilledSize;

    private long matchOrderId;

    private long matchOrderUserId;

    private int version;

    private ResultCode resultCode;


    protected OrderMatch() {

    }


    public void from(Sequence sequence, OrderCommand orderCommand) {

        this.sequence = sequence.getAndIncrement();
        this.orderId = orderCommand.getOrderId();
        this.symbolId = orderCommand.getSymbolId();
        this.userId = orderCommand.getUserId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderAction = orderCommand.getOrderAction();
        this.orderType = orderCommand.getOrderType();
        this.orderPrice = orderCommand.getPrice();
        this.matchPrice = -1L;
        this.depthSize = -1L;
        this.totalSize = orderCommand.getSize();
        this.accFilledSize = 0L;
        this.matchFilledSize = 0L;
        this.matchOrderId = 0L;
        this.matchOrderUserId = 0L;
        this.version = 0;
        this.resultCode = null;
    }


    public void from(Sequence sequence, OrderCommand orderCommand, Order order, long depthSize) {

        this.sequence = sequence.getAndIncrement();
        this.orderId = orderCommand.getOrderId();
        this.symbolId = orderCommand.getSymbolId();
        this.userId = orderCommand.getUserId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderAction = orderCommand.getOrderAction();
        this.orderType = orderCommand.getOrderType();
        this.orderPrice = orderCommand.getPrice();
        this.matchPrice = -1L;
        this.depthSize = depthSize;
        this.totalSize = orderCommand.getSize();
        this.accFilledSize = order.getFilledSize();
        this.matchFilledSize = 0L;
        this.matchOrderId = 0L;
        this.matchOrderUserId = 0L;
        this.version = order.getVersion();
        this.resultCode = null;
    }


    public void from(Sequence sequence, Order order, int symbolId, OrderAction orderAction, long orderPrice, long matchPrice, long matchFilledSize,
                     Order thatOrder, long depthSize, long timestamp) {

        this.sequence = sequence.getAndIncrement();
        this.orderId = order.getOrderId();
        this.symbolId = symbolId;
        this.userId = order.getUserId();
        this.timestamp = timestamp;
        this.orderAction = orderAction;
        this.orderType = order.getOrderType();
        this.orderPrice = orderPrice;
        this.matchPrice = matchPrice;
        this.depthSize = depthSize;
        this.totalSize = order.getRemainSize() + order.getFilledSize();
        this.accFilledSize = order.getFilledSize();
        this.matchFilledSize = matchFilledSize;
        this.matchOrderId = thatOrder.getOrderId();
        this.matchOrderUserId = thatOrder.getUserId();
        this.version = order.getVersion();
        this.resultCode = ResultCode.MATCH;
    }


    @Override public void clearData() {

        //        this.sequence = 0L;
        //        this.orderId = 0L;
        //        this.symbolId = 0;
        //        this.userId = 0L;
        //        this.timestamp = 0L;
        //        this.orderAction = null;
        //        this.orderType = null;
        //        this.orderPrice = 0L;
        //        this.matchPrice = 0L;
        //        this.depthSize = 0L;
        //        this.totalSize = 0L;
        //        this.accFilledSize = 0L;
        //        this.matchFilledSize = 0L;
        //        this.matchOrderId = 0L;
        //        this.matchOrderUserId = 0L;
        //        this.version = 0;
        //        this.resultCode = null;
    }


    @Override public byte[] getBytes() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(109);
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(this.sequence);
        byteBuffer.putLong(this.orderId);
        byteBuffer.putInt(this.symbolId);
        byteBuffer.putLong(this.userId);
        byteBuffer.putLong(this.timestamp);
        byteBuffer.put(this.orderAction.code);
        byteBuffer.put(this.orderType.code);
        byteBuffer.putLong(this.orderPrice);
        byteBuffer.putLong(this.matchPrice);
        byteBuffer.putLong(this.depthSize);
        byteBuffer.putLong(this.totalSize);
        byteBuffer.putLong(this.accFilledSize);
        byteBuffer.putLong(this.matchFilledSize);
        byteBuffer.putLong(this.matchOrderId);
        byteBuffer.putLong(this.matchOrderUserId);
        byteBuffer.putInt(this.version);
        byteBuffer.putShort((short) this.resultCode.code);
        return byteBuffer.array();
    }
}
