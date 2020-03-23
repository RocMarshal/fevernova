package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.task.exchange.data.Sequence;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.condition.ConditionOrder;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderArray;
import com.github.fevernova.task.exchange.data.order.OrderMode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.Validate;

import java.nio.ByteBuffer;


@Getter
@Setter
@ToString
public class OrderMatch implements Data {


    private int symbolId;

    private long timestamp;

    private OrderMode orderMode;

    private ResultCode resultCode;

    private OrderMatchPart part0;

    private OrderMatchPart part1;

    private long matchPrice;

    private long matchSize;

    private OrderAction driverAction;


    protected OrderMatch() {

    }


    //PLACE OR CANCEL
    public void from(Sequence sequence, OrderCommand orderCommand, Order order, OrderArray orderArray) {

        this.symbolId = orderCommand.getSymbolId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderMode = orderCommand.getOrderMode();
        this.resultCode = null;

        this.part0.setSequence(sequence.getAndIncrement());
        this.part0.setOrderId(order.getOrderId());
        this.part0.setUserId(order.getUserId());
        this.part0.setOrderAction(orderArray.getOrderAction());
        this.part0.setOrderType(order.getOrderType());
        this.part0.setOrderPrice(orderArray.getPrice());
        this.part0.setOrderPriceDepthSize(orderArray.getSize());
        this.part0.setOrderPriceOrderCount(orderArray.getQueue().size());
        this.part0.setOrderTotalSize(order.getRemainSize() + order.getFilledSize());
        this.part0.setOrderAccFilledSize(order.getFilledSize());
        this.part0.setOrderVersion(order.getVersion());

        this.part1.clearData();

        this.matchPrice = -1L;
        this.matchSize = 0L;
        this.driverAction = null;
    }


    //FOK CANCEL or POSTONLY CANCEL or HEARTBEAT
    public void from(Sequence sequence, OrderCommand orderCommand) {

        this.symbolId = orderCommand.getSymbolId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderMode = orderCommand.getOrderMode();
        this.resultCode = null;

        this.part0.setSequence(sequence.getAndIncrement());
        this.part0.setOrderId(orderCommand.getOrderId());
        this.part0.setUserId(orderCommand.getUserId());
        this.part0.setOrderAction(orderCommand.getOrderAction());
        this.part0.setOrderType(orderCommand.getOrderType());
        this.part0.setOrderPrice(orderCommand.getPrice());
        this.part0.setOrderPriceDepthSize(-1L);
        this.part0.setOrderPriceOrderCount(-1);
        this.part0.setOrderTotalSize(orderCommand.getSize());
        this.part0.setOrderAccFilledSize(0L);
        this.part0.setOrderVersion(1);

        this.part1.clearData();

        this.matchPrice = -1L;
        this.matchSize = 0L;
        this.driverAction = null;
    }


    //CANCEL BY DEPTH ONLY
    public void from(Sequence sequence, int symbolId, Order order, OrderArray orderArray, long timestamp) {

        this.symbolId = symbolId;
        this.timestamp = timestamp;
        this.orderMode = OrderMode.SIMPLE;
        this.resultCode = ResultCode.CANCEL_DEPTHONLY;

        this.part0.setSequence(sequence.getAndIncrement());
        this.part0.setOrderId(order.getOrderId());
        this.part0.setUserId(order.getUserId());
        this.part0.setOrderAction(orderArray.getOrderAction());
        this.part0.setOrderType(order.getOrderType());
        this.part0.setOrderPrice(orderArray.getPrice());
        this.part0.setOrderPriceDepthSize(orderArray.getSize());
        this.part0.setOrderPriceOrderCount(orderArray.getQueue().size());
        this.part0.setOrderTotalSize(order.getRemainSize() + order.getFilledSize());
        this.part0.setOrderAccFilledSize(order.getFilledSize());
        this.part0.setOrderVersion(order.getVersion());

        this.part1.clearData();

        this.matchPrice = -1L;
        this.matchSize = 0L;
        this.driverAction = null;
    }


    //MATCH
    public void from(Sequence sequence, int symbolId, Order order, Order thatOrder, OrderArray orderArray, OrderArray thatOrderArray,
                     long matchPrice, long matchSize, long timestamp, OrderAction driverAction) {

        this.symbolId = symbolId;
        this.timestamp = timestamp;
        this.orderMode = OrderMode.SIMPLE;
        this.resultCode = ResultCode.MATCH;

        this.matchPrice = matchPrice;
        this.matchSize = matchSize;
        this.driverAction = driverAction;

        this.part0.setSequence(sequence.getAndIncrement());
        this.part0.setOrderId(order.getOrderId());
        this.part0.setUserId(order.getUserId());
        this.part0.setOrderAction(orderArray.getOrderAction());
        this.part0.setOrderType(order.getOrderType());
        this.part0.setOrderPrice(orderArray.getPrice());
        this.part0.setOrderPriceDepthSize(orderArray.getSize());
        this.part0.setOrderPriceOrderCount(orderArray.getQueue().size());
        this.part0.setOrderTotalSize(order.getRemainSize() + order.getFilledSize());
        this.part0.setOrderAccFilledSize(order.getFilledSize());
        this.part0.setOrderVersion(order.getVersion());

        this.part1.setSequence(sequence.getAndIncrement());
        this.part1.setOrderId(thatOrder.getOrderId());
        this.part1.setUserId(thatOrder.getUserId());
        this.part1.setOrderAction(thatOrderArray.getOrderAction());
        this.part1.setOrderType(thatOrder.getOrderType());
        this.part1.setOrderPrice(thatOrderArray.getPrice());
        this.part1.setOrderPriceDepthSize(thatOrderArray.getSize());
        this.part1.setOrderPriceOrderCount(thatOrderArray.getQueue().size());
        this.part1.setOrderTotalSize(thatOrder.getRemainSize() + thatOrder.getFilledSize());
        this.part1.setOrderAccFilledSize(thatOrder.getFilledSize());
        this.part1.setOrderVersion(thatOrder.getVersion());
    }


    //Condition PLACE OR CANCEL
    public void from(Sequence sequence, OrderCommand orderCommand, ConditionOrder order) {

        this.symbolId = orderCommand.getSymbolId();
        this.timestamp = orderCommand.getTimestamp();
        this.orderMode = orderCommand.getOrderMode();
        this.resultCode = null;

        this.part0.setSequence(sequence.getAndIncrement());
        this.part0.setOrderId(order.getOrderId());
        this.part0.setUserId(order.getUserId());
        this.part0.setOrderAction(order.getOrderAction());
        this.part0.setOrderType(order.getOrderType());
        this.part0.setOrderPrice(order.getPrice());
        this.part0.setOrderPriceDepthSize(-1L);
        this.part0.setOrderPriceOrderCount(0);
        this.part0.setOrderTotalSize(order.getSize());
        this.part0.setOrderAccFilledSize(0L);
        this.part0.setOrderVersion(order.getVersion());

        this.part1.clearData();

        this.matchPrice = -1L;
        this.matchSize = 0L;
        this.driverAction = null;
    }


    public void from(byte[] bytes) {

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byte version = byteBuffer.get();
        Validate.isTrue(version == 0);
        this.symbolId = byteBuffer.getInt();
        this.timestamp = byteBuffer.getLong();
        this.orderMode = OrderMode.of(byteBuffer.get());
        this.resultCode = ResultCode.of(byteBuffer.getShort());

        this.part0.from(byteBuffer);

        if (ResultCode.MATCH == this.resultCode) {
            this.part1.from(byteBuffer);
            this.matchPrice = byteBuffer.getLong();
            this.matchSize = byteBuffer.getLong();
            this.driverAction = OrderAction.of(byteBuffer.get());
        }
    }


    @Override public byte[] getBytes() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(16 + 66 + (ResultCode.MATCH == this.resultCode ? 83 : 0));
        byteBuffer.put((byte) 0);
        byteBuffer.putInt(this.symbolId);
        byteBuffer.putLong(this.timestamp);
        byteBuffer.put(this.orderMode.code);
        byteBuffer.putShort((short) this.resultCode.code);

        this.part0.getBytes(byteBuffer);

        if (ResultCode.MATCH == this.resultCode) {
            this.part1.getBytes(byteBuffer);
            byteBuffer.putLong(this.matchPrice);
            byteBuffer.putLong(this.matchSize);
            byteBuffer.put(this.driverAction.code);
        }
        return byteBuffer.array();
    }


    @Override public void clearData() {

        //this.symbolId = 0;
        //this.timestamp = 0L;
        //this.orderMode = null;
        //this.resultCode = null;
        //this.part0.clearData();
        //this.part1.clearData();
        //this.matchPrice = 0L;
        //this.matchSize = 0L;
        //this.driverAction = null;
    }


    public long maxSeq() {

        return Math.max(this.part0.getSequence(), this.part1.getSequence());
    }
}
