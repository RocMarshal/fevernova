package com.github.fevernova.task.exchange.data.order;


import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
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
public final class Order implements WriteBytesMarshallable, Comparable<Order> {


    private long orderId;

    private long userId;

    private OrderType orderType;

    private long remainSize;

    private long filledSize;

    private int version;


    public Order(BytesIn bytes) {

        this.orderId = bytes.readLong();
        this.userId = bytes.readLong();
        this.orderType = OrderType.GTC;
        this.remainSize = bytes.readLong();
        this.filledSize = bytes.readLong();
        this.version = bytes.readInt();
    }


    public Order(OrderCommand orderCommand) {

        this.orderId = orderCommand.getOrderId();
        this.userId = orderCommand.getUserId();
        this.orderType = orderCommand.getOrderType();
        this.remainSize = orderCommand.getSize();
        this.filledSize = 0L;
        this.version = 0;
    }


    public void decrement(long delta) {

        this.remainSize -= delta;
        this.filledSize += delta;
        this.version++;
    }


    public boolean needIOCClear() {

        return this.orderType == OrderType.IOC && this.remainSize > 0;
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.orderId);
        bytes.writeLong(this.userId);
        bytes.writeLong(this.remainSize);
        bytes.writeLong(this.filledSize);
        bytes.writeInt(this.version);
    }


    @Override public int compareTo(@NotNull Order o) {

        return Long.compare(this.orderId, o.getOrderId());
    }
}
