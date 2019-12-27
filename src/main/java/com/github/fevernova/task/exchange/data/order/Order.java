package com.github.fevernova.task.exchange.data.order;


import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.engine.OrderArray;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


@Getter
@Setter
@NoArgsConstructor
public final class Order implements WriteBytesMarshallable {


    private long orderId;

    private long userId;

    private OrderType orderType;

    private long remainSize;

    private long filledSize;

    private int version;

    private OrderArray link;


    public Order(BytesIn bytes, OrderArray link) {

        this.orderId = bytes.readLong();
        this.userId = bytes.readLong();
        this.orderType = OrderType.GTC;
        this.remainSize = bytes.readLong();
        this.filledSize = bytes.readLong();
        this.version = bytes.readInt();
        this.link = link;
    }


    public Order(OrderCommand orderCommand) {

        this.orderId = orderCommand.getOrderId();
        this.userId = orderCommand.getUserId();
        this.orderType = orderCommand.getOrderType();
        this.remainSize = orderCommand.getSize();
        this.filledSize = 0L;
        this.version = 0;
    }


    public OrderMatch decrement(int symbolId, long price, long delta, long otherOrderId) {

        this.remainSize -= delta;
        this.filledSize += delta;
        this.version++;
        OrderMatch result = new OrderMatch();
        result.from(this, symbolId, price, delta, otherOrderId);
        return result;
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.orderId);
        bytes.writeLong(this.userId);
        bytes.writeLong(this.remainSize);
        bytes.writeLong(this.filledSize);
        bytes.writeInt(this.version);
    }
}
