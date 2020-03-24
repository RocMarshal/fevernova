package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.nio.ByteBuffer;


@Getter
@Setter
@ToString
public class OrderPart {


    private long sequence;

    private long orderId;

    private long userId;

    private OrderAction orderAction;

    private OrderType orderType;

    private long orderPrice;

    private long orderPriceDepthSize;

    private int orderPriceOrderCount;

    private long orderTotalSize;

    private long orderAccFilledSize;

    private int orderVersion;


    public void from(ByteBuffer byteBuffer) {

        this.sequence = byteBuffer.getLong();
        this.orderId = byteBuffer.getLong();
        this.userId = byteBuffer.getLong();
        this.orderAction = OrderAction.of(byteBuffer.get());
        this.orderType = OrderType.of(byteBuffer.get());
        this.orderPrice = byteBuffer.getLong();
        this.orderPriceDepthSize = byteBuffer.getLong();
        this.orderPriceOrderCount = byteBuffer.getInt();
        this.orderTotalSize = byteBuffer.getLong();
        this.orderAccFilledSize = byteBuffer.getLong();
        this.orderVersion = byteBuffer.getInt();
    }


    public void getBytes(ByteBuffer byteBuffer) {

        byteBuffer.putLong(this.sequence);
        byteBuffer.putLong(this.orderId);
        byteBuffer.putLong(this.userId);
        byteBuffer.put(this.orderAction.code);
        byteBuffer.put(this.orderType.code);
        byteBuffer.putLong(this.orderPrice);
        byteBuffer.putLong(this.orderPriceDepthSize);
        byteBuffer.putInt(this.orderPriceOrderCount);
        byteBuffer.putLong(this.orderTotalSize);
        byteBuffer.putLong(this.orderAccFilledSize);
        byteBuffer.putInt(this.orderVersion);
    }


    public void clearData() {

        this.sequence = 0L;
        this.orderId = 0L;
        this.userId = 0L;
        this.orderAction = null;
        this.orderType = null;
        this.orderPrice = 0L;
        this.orderPriceDepthSize = 0L;
        this.orderPriceOrderCount = 0;
        this.orderTotalSize = 0L;
        this.orderAccFilledSize = 0L;
        this.orderVersion = 0;
    }
}
