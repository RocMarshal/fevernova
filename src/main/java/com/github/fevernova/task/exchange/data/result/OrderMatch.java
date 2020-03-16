package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.task.exchange.data.Sequence;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.engine.OrderArray;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.Validate;

import java.nio.ByteBuffer;


@Getter
@Setter
@ToString
public class OrderMatch implements Data {


    private long sequence;

    private int symbolId;

    private long orderId;

    private long userId;

    private long timestamp;

    private OrderAction orderAction;

    private OrderType orderType;

    private long orderPrice;

    private long orderPriceDepthSize;

    private int orderPriceOrderCount;

    private long orderTotalSize;

    private long orderAccFilledSize;

    private int orderVersion;

    private long matchPrice;

    private long matchSize;

    private long matchOrderId;

    private long matchOrderUserId;

    private OrderAction driverAction;

    private ResultCode resultCode;


    protected OrderMatch() {

    }


    //FOK CANCEL or HEARTBEAT
    public void from(Sequence sequence, OrderCommand orderCommand) {

        this.sequence = sequence.getAndIncrement();
        this.symbolId = orderCommand.getSymbolId();
        this.orderId = orderCommand.getOrderId();
        this.userId = orderCommand.getUserId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderAction = orderCommand.getOrderAction();
        this.orderType = orderCommand.getOrderType();
        this.orderPrice = orderCommand.getPrice();
        this.orderPriceDepthSize = -1L;
        this.orderPriceOrderCount = 0;
        this.orderTotalSize = orderCommand.getSize();
        this.orderAccFilledSize = 0L;
        this.orderVersion = 0;
        this.matchPrice = -1L;
        this.matchSize = 0L;
        this.matchOrderId = 0L;
        this.matchOrderUserId = 0L;
        this.driverAction = orderCommand.getOrderAction();
        this.resultCode = null;
    }


    //PLACE OR CANCEL
    public void from(Sequence sequence, OrderCommand orderCommand, Order order, OrderArray orderArray) {

        this.sequence = sequence.getAndIncrement();
        this.symbolId = orderCommand.getSymbolId();
        this.orderId = orderCommand.getOrderId();
        this.userId = orderCommand.getUserId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderAction = orderCommand.getOrderAction();
        this.orderType = orderCommand.getOrderType();
        this.orderPrice = orderCommand.getPrice();
        this.orderPriceDepthSize = orderArray.getSize();
        this.orderPriceOrderCount = orderArray.getQueue().size();
        this.orderTotalSize = orderCommand.getSize();
        this.orderAccFilledSize = order.getFilledSize();
        this.orderVersion = order.getVersion();
        this.matchPrice = -1L;
        this.matchSize = 0L;
        this.matchOrderId = 0L;
        this.matchOrderUserId = 0L;
        this.driverAction = orderCommand.getOrderAction();
        this.resultCode = null;
    }


    //MATCH
    public void from(Sequence sequence, int symbolId, Order order, Order thatOrder, OrderArray orderArray,
                     long matchPrice, long matchSize, long timestamp, OrderAction driverAction) {

        this.sequence = sequence.getAndIncrement();
        this.symbolId = symbolId;
        this.orderId = order.getOrderId();
        this.userId = order.getUserId();
        this.timestamp = timestamp;
        this.orderAction = orderArray.getOrderAction();
        this.orderType = order.getOrderType();
        this.orderPrice = orderArray.getPrice();
        this.orderPriceDepthSize = orderArray.getSize();
        this.orderPriceOrderCount = orderArray.getQueue().size();
        this.orderTotalSize = order.getRemainSize() + order.getFilledSize();
        this.orderAccFilledSize = order.getFilledSize();
        this.orderVersion = order.getVersion();
        this.matchPrice = matchPrice;
        this.matchSize = matchSize;
        this.matchOrderId = thatOrder.getOrderId();
        this.matchOrderUserId = thatOrder.getUserId();
        this.driverAction = driverAction;
        this.resultCode = ResultCode.MATCH;
    }


    public void from(byte[] bytes) {

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byte version = byteBuffer.get();
        Validate.isTrue(version == 0);
        this.sequence = byteBuffer.getLong();
        this.symbolId = byteBuffer.getInt();
        this.orderId = byteBuffer.getLong();
        this.userId = byteBuffer.getLong();
        this.timestamp = byteBuffer.getLong();
        this.orderAction = OrderAction.of(byteBuffer.get());
        this.orderType = OrderType.of(byteBuffer.get());
        this.orderPrice = byteBuffer.getLong();
        this.orderPriceDepthSize = byteBuffer.getLong();
        this.orderPriceOrderCount = byteBuffer.getInt();
        this.orderTotalSize = byteBuffer.getLong();
        this.orderAccFilledSize = byteBuffer.getLong();
        this.orderVersion = byteBuffer.getInt();
        this.matchPrice = byteBuffer.getLong();
        this.matchSize = byteBuffer.getLong();
        this.matchOrderId = byteBuffer.getLong();
        this.matchOrderUserId = byteBuffer.getLong();
        this.driverAction = OrderAction.of(byteBuffer.get());
        this.resultCode = ResultCode.of(byteBuffer.getShort());
    }


    @Override public byte[] getBytes() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(114);
        byteBuffer.put((byte) 0);
        byteBuffer.putLong(this.sequence);
        byteBuffer.putInt(this.symbolId);
        byteBuffer.putLong(this.orderId);
        byteBuffer.putLong(this.userId);
        byteBuffer.putLong(this.timestamp);
        byteBuffer.put(this.orderAction.code);
        byteBuffer.put(this.orderType.code);
        byteBuffer.putLong(this.orderPrice);
        byteBuffer.putLong(this.orderPriceDepthSize);
        byteBuffer.putInt(this.orderPriceOrderCount);
        byteBuffer.putLong(this.orderTotalSize);
        byteBuffer.putLong(this.orderAccFilledSize);
        byteBuffer.putInt(this.orderVersion);
        byteBuffer.putLong(this.matchPrice);
        byteBuffer.putLong(this.matchSize);
        byteBuffer.putLong(this.matchOrderId);
        byteBuffer.putLong(this.matchOrderUserId);
        byteBuffer.put(this.driverAction.code);
        byteBuffer.putShort((short) this.resultCode.code);
        return byteBuffer.array();
    }


    @Override public void clearData() {

        //this.sequence = 0L;
        //this.symbolId = 0;
        //this.orderId = 0L;
        //this.userId = 0L;
        //this.timestamp = 0L;
        //this.orderAction = null;
        //this.orderType = null;
        //this.orderPrice = 0L;
        //this.orderPriceDepthSize = 0L;
        //this.orderPriceOrderCount = 0;
        //this.orderTotalSize = 0L;
        //this.orderAccFilledSize = 0L;
        //this.orderVersion = 0;
        //this.matchPrice = 0L;
        //this.matchSize = 0L;
        //this.matchOrderId = 0L;
        //this.matchOrderUserId = 0L;
        //this.driverAction = null;
        //this.resultCode = null;
    }
}
