package com.github.fevernova.task.exchange.data.order.condition;


import com.github.fevernova.framework.common.structure.queue.LinkedObject;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.jetbrains.annotations.NotNull;


@Getter
@Setter
@NoArgsConstructor
public class ConditionOrder extends LinkedObject<ConditionOrder> implements WriteBytesMarshallable, Comparable<ConditionOrder> {


    private long orderId;

    private long userId;

    private OrderType orderType;

    private OrderAction orderAction;

    private long price;

    private long size;

    private int version;


    public ConditionOrder(BytesIn bytes) {

        this.orderId = bytes.readLong();
        this.userId = bytes.readLong();
        this.orderType = OrderType.of(bytes.readByte());
        this.orderAction = OrderAction.of(bytes.readByte());
        this.price = bytes.readLong();
        this.size = bytes.readLong();
        this.version = bytes.readInt();
    }


    public ConditionOrder(OrderCommand orderCommand) {

        this.orderId = orderCommand.getOrderId();
        this.userId = orderCommand.getUserId();
        this.orderType = orderCommand.getOrderType();
        this.orderAction = orderCommand.getOrderAction();
        this.price = orderCommand.getPrice();
        this.size = orderCommand.getSize();
        this.version = 1;
    }


    public void cancel() {

        this.version++;
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.orderId);
        bytes.writeLong(this.userId);
        bytes.writeByte(this.orderType.code);
        bytes.writeByte(this.orderAction.code);
        bytes.writeLong(this.price);
        bytes.writeLong(this.size);
        bytes.writeInt(this.version);
    }


    @Override public int compareTo(@NotNull ConditionOrder o) {

        return Long.compare(this.orderId, o.getOrderId());
    }
}
