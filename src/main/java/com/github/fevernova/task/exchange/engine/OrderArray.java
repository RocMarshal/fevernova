package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.google.common.collect.Lists;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.LinkedList;
import java.util.List;


@Getter
public final class OrderArray implements WriteBytesMarshallable {


    private final LinkedList<Order> queue = Lists.newLinkedList();

    private final OrderAction orderAction;

    private long price;

    private long size;


    public OrderArray(BytesIn bytes) {

        this.orderAction = OrderAction.of(bytes.readByte());
        this.price = bytes.readLong();
        int length = bytes.readInt();
        for (int i = 0; i < length; i++) {
            Order order = new Order(bytes);
            this.queue.add(order);
            this.size += order.getRemainSize();
        }
    }


    public OrderArray(OrderAction orderAction, long price) {

        this.orderAction = orderAction;
        this.price = price;
    }


    public void addOrder(Order order) {

        this.queue.add(order);
        this.size += order.getRemainSize();
    }


    public void remove(Order order) {

        this.queue.remove(order);
        this.size -= order.getRemainSize();
    }


    public void decr(Order order, long delta) {

        this.size -= delta;
        if (order.getRemainSize() == 0) {
            this.queue.remove(order);
        }
    }


    public Order findOrder(long orderId) {

        for (Order order : this.queue) {
            if (order.getOrderId() == orderId) {
                return order;
            }
        }
        return null;
    }


    public void meet(OrderArray other, int symbolId, long matchPrice, List<OrderMatch> result) {

        do {
            Order thisOrder = this.queue.getFirst();
            Order thatOrder = other.getQueue().getFirst();
            long delta = Math.min(thisOrder.getRemainSize(), thatOrder.getRemainSize());
            result.add(thisOrder.decrement(symbolId, this.orderAction, matchPrice, delta, thatOrder.getOrderId()));
            result.add(thatOrder.decrement(symbolId, other.orderAction, matchPrice, delta, thisOrder.getOrderId()));
            other.decr(thatOrder, delta);
            this.decr(thisOrder, delta);
        } while (other.getSize() > 0L);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeByte(this.orderAction.code);
        bytes.writeLong(this.price);
        bytes.writeInt(this.queue.size());
        this.queue.forEach(order -> order.writeMarshallable(bytes));
    }
}
