package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.framework.common.structure.queue.LinkedQueue;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


@Getter
public final class OrderArray implements WriteBytesMarshallable {


    private final LinkedQueue<Order> queue = new LinkedQueue<>();

    private final OrderAction orderAction;

    private long price;

    private long size;


    public OrderArray(BytesIn bytes) {

        this.orderAction = OrderAction.of(bytes.readByte());
        this.price = bytes.readLong();
        int length = bytes.readInt();
        for (int i = 0; i < length; i++) {
            Order order = new Order(bytes);
            this.queue.offer(order);
            this.size += order.getRemainSize();
        }
    }


    public OrderArray(OrderAction orderAction, long price) {

        this.orderAction = orderAction;
        this.price = price;
    }


    public void addOrder(Order order) {

        this.queue.offer(order);
        this.size += order.getRemainSize();
    }


    public Order findAndRemoveOrder(long orderId) {

        Order r = this.queue.findAndRemove(order -> order.getOrderId() == orderId);
        if (r != null) {
            this.size -= r.getRemainSize();
        }
        return r;
    }


    public void meet(OrderArray that, int symbolId, long matchPrice, DataProvider<Long, OrderMatch> provider) {

        do {
            Order thisOrder = this.queue.peek();
            Order thatOrder = that.queue.peek();
            long delta = Math.min(thisOrder.getRemainSize(), thatOrder.getRemainSize());
            thisOrder.decrement(delta);
            thatOrder.decrement(delta);
            that.decrement(thatOrder, delta);
            this.decrement(thisOrder, delta);
            OrderMatch thisOrderMatch = provider.feedOne(thisOrder.getOrderId());
            thisOrderMatch.from(thisOrder, symbolId, this.orderAction, matchPrice, delta, thatOrder);
            provider.push();
            OrderMatch thatOrderMatch = provider.feedOne(thatOrder.getOrderId());
            thatOrderMatch.from(thatOrder, symbolId, that.orderAction, matchPrice, delta, thisOrder);
            provider.push();
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
        for (Order order : this.queue) {
            order.writeMarshallable(bytes);
        }
    }
}
