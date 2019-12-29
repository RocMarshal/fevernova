package com.github.fevernova.task.exchange.data.cmd;


import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.Validate;

import java.nio.ByteBuffer;


@Getter
@Setter
@ToString
public class OrderCommand {


    private OrderCommandType orderCommandType;

    private long orderId;

    private int symbolId;

    private long userId;

    private long timestamp;

    private OrderAction orderAction;

    private OrderType orderType;

    private long price;

    private long size;


    public void from(byte[] bytes) {

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byte version = byteBuffer.get();
        Validate.isTrue(version == 0);
        this.orderCommandType = OrderCommandType.of(byteBuffer.get());
        this.orderId = byteBuffer.getLong();
        this.symbolId = byteBuffer.getInt();
        this.userId = byteBuffer.getLong();
        this.timestamp = byteBuffer.getLong();
        this.orderAction = OrderAction.of(byteBuffer.get());
        this.orderType = OrderType.of(byteBuffer.get());
        this.price = byteBuffer.getLong();
        this.size = byteBuffer.getLong();
    }

}
