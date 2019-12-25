package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.google.common.collect.Lists;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.Validate;
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


    public void remove(Order order, long delta) {

        this.queue.remove(order);
        this.size -= delta;
    }


    public Order findOrder(long orderId) {

        for (Order order : this.queue) {
            if (order.getOrderId() == orderId) {
                return order;
            }
        }
        return null;
    }


    public Order poll() {

        Validate.isTrue(!this.queue.isEmpty());
        return this.queue.getFirst();
    }


    public List<OrderMatch> meet(OrderArray other, int symbolId) {

        List<OrderMatch> result = Lists.newArrayList();
        while (!other.isEmpty()) {
            Order thisOrder = poll();
            long thisOrderSize = thisOrder.getRemainSize();
            Order thatOrder = other.poll();
            long thatOrderSize = thatOrder.getRemainSize();
            Pair<OrderMatch, OrderMatch> pair = thisOrder.meet(thatOrder, symbolId, this.price);//TODO price规则待定
            result.add(pair.getKey());
            result.add(pair.getValue());
            if (thatOrder.KO()) {
                other.remove(thatOrder, thatOrderSize);
            }
            if (thisOrder.KO()) {
                remove(thisOrder, thisOrderSize);
            }
        }
        return result;
    }


    public boolean isEmpty() {

        Validate.isTrue((this.size > 0 ^ this.queue.isEmpty()));
        return this.size == 0L && this.queue.isEmpty();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.orderAction.code);
        bytes.writeLong(this.price);
        bytes.writeLong(this.size);
        bytes.writeInt(this.queue.size());
        this.queue.forEach(order -> order.writeMarshallable(bytes));
    }
}
