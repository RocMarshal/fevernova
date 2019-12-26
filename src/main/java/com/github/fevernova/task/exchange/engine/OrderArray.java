package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.google.common.collect.Lists;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;


@Getter
public class OrderArray implements WriteBytesMarshallable {


    private final LinkedList<Order> queue = Lists.newLinkedList();

    private final OrderAction orderAction;

    private long price;

    private long size;


    public OrderArray(BytesIn bytes) {

        this.orderAction = OrderAction.of(bytes.readInt());
        this.price = bytes.readLong();
        this.size = bytes.readLong();
        int length = bytes.readInt();
        for (int i = 0; i < length; i++) {
            this.queue.add(new Order(bytes, this));
        }
    }


    public OrderArray(OrderAction orderAction, long price) {

        this.orderAction = orderAction;
        this.price = price;
    }


    public void addOrder(Order order) {

        order.setLink(this);
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


    public List<OrderMatch> meet(OrderArray other, int symbolId) {

        List<OrderMatch> result = Lists.newArrayList();
        do {
            Order thisOrder = this.queue.getFirst();
            Order thatOrder = other.getQueue().getFirst();
            Pair<OrderMatch, OrderMatch> pair = thisOrder.meet(thatOrder, symbolId, this.price);//TODO price规则待定
            result.add(pair.getKey());
            result.add(pair.getValue());
            other.decr(thatOrder, pair.getRight().getMatchFilledSize());
            decr(thisOrder, pair.getLeft().getMatchFilledSize());
        } while (other.getSize() > 0L);
        return result;
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.orderAction.code);
        bytes.writeLong(this.price);
        bytes.writeLong(this.size);
        bytes.writeInt(this.queue.size());
        this.queue.forEach(order -> order.writeMarshallable(bytes));
    }
}
