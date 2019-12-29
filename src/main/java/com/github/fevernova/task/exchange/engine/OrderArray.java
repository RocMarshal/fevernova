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


    public void removeOrder(Order order) {

        this.queue.remove(order);
        this.size -= order.getRemainSize();
    }


    public Order findOrder(long orderId) {

        for (Order order : this.queue) {
            if (order.getOrderId() == orderId) {
                return order;
            }
        }
        return null;
    }


    public void meet(OrderArray that, int symbolId, long matchPrice, List<OrderMatch> result) {

        do {
            Order thisOrder = this.queue.getFirst();
            Order thatOrder = that.queue.getFirst();
            long delta = Math.min(thisOrder.getRemainSize(), thatOrder.getRemainSize());
            thisOrder.decrement(delta);
            thatOrder.decrement(delta);
            OrderMatch thisOrderMatch = new OrderMatch();
            OrderMatch thatOrderMatch = new OrderMatch();
            thisOrderMatch.from(thisOrder, symbolId, this.orderAction, matchPrice, delta, thatOrder);
            thatOrderMatch.from(thatOrder, symbolId, that.orderAction, matchPrice, delta, thisOrder);
            result.add(thisOrderMatch);
            result.add(thatOrderMatch);
            that.decrement(thatOrder, delta);
            this.decrement(thisOrder, delta);
        } while (that.getSize() > 0L);
    }


    private void decrement(Order order, long delta) {

        this.size -= delta;
        if (order.getRemainSize() == 0) {
            this.queue.remove(order);
        }
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeByte(this.orderAction.code);
        bytes.writeLong(this.price);
        bytes.writeInt(this.queue.size());
        this.queue.forEach(order -> order.writeMarshallable(bytes));
    }
}
