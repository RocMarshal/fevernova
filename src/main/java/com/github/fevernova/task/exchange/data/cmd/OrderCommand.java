package com.github.fevernova.task.exchange.data.cmd;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@Setter
@ToString
public class OrderCommand implements Data {


    private OrderCommandType orderCommandType;

    private long orderId;

    private int symbolId;

    private long userId;

    private long timestamp;

    private OrderAction orderAction;

    private OrderType orderType;

    private long price;

    private long size;


    @Override public void clearData() {

        this.orderCommandType = null;
        this.orderId = 0L;
        this.symbolId = 0;
        this.userId = 0L;
        this.timestamp = 0L;
        this.orderAction = null;
        this.orderType = null;
        this.price = 0L;
        this.size = 0L;
    }


    @Override public byte[] getBytes() {

        return new byte[0];
    }
}
