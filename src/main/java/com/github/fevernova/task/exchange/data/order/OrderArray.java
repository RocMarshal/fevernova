package com.github.fevernova.task.exchange.data.order;


import com.github.fevernova.framework.common.structure.queue.LinkedQueue;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.Sequence;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import lombok.Getter;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


@Getter
public final class OrderArray implements WriteBytesMarshallable {


    private final LinkedQueue<Order> queue = new LinkedQueue<>();

    private final OrderAction orderAction;

    private long price;

    private long depthOnlySize;

    private long size;

    @Setter
    private boolean lazy = false;


    public OrderArray(BytesIn bytes) {

        this.orderAction = OrderAction.of(bytes.readByte());
        this.price = bytes.readLong();
        int length = bytes.readInt();
        for (int i = 0; i < length; i++) {
            Order order = new Order(bytes);
            this.queue.offer(order);
            this.size += order.getRemainSize();
            this.depthOnlySize += (OrderType.DEPTHONLY == order.getOrderType() ? order.getRemainSize() : 0);
        }
    }


    public OrderArray(OrderAction orderAction, long price, boolean lazy) {

        this.orderAction = orderAction;
        this.price = price;
        this.lazy = lazy;
    }


    public long getSizeWithoutDepthOnly() {

        return this.size - this.depthOnlySize;
    }


    public void addOrder(Order order) {

        this.queue.offer(order);
        this.size += order.getRemainSize();
        this.depthOnlySize += (OrderType.DEPTHONLY == order.getOrderType() ? order.getRemainSize() : 0);
    }


    public Order findAndRemoveOrder(long orderId) {

        Order r = this.queue.findAndRemove(order -> order.getOrderId() == orderId);
        if (r != null) {
            this.size -= r.getRemainSize();
            r.cancel();
        }
        return r;
    }


    public void meet(Sequence sequence, OrderArray that, int symbolId, long matchPrice, DataProvider<Integer, OrderMatch> provider, long timestamp,
                     OrderAction driverAction) {

        do {
            Order thisOrder = this.queue.peek();
            Order thatOrder = that.queue.peek();

            if (cancelDepthOnlyOrder(sequence, symbolId, timestamp, thisOrder, this, provider)
                || cancelDepthOnlyOrder(sequence, symbolId, timestamp, thatOrder, that, provider)) {
                return;
            }

            long delta = Math.min(thisOrder.getRemainSize(), thatOrder.getRemainSize());
            thisOrder.decrement(delta);
            thatOrder.decrement(delta);
            that.decrement(thatOrder, delta);
            this.decrement(thisOrder, delta);
            OrderMatch orderMatch = provider.feedOne(symbolId);
            orderMatch.from(sequence, symbolId, thisOrder, thatOrder, this, that, matchPrice, delta, timestamp, driverAction);
            provider.push();
        } while (this.getSize() > 0 && that.getSize() > 0L);
    }


    private static boolean cancelDepthOnlyOrder(Sequence sequence, int symbolId, long timestamp, Order order, OrderArray orderArray,
                                                DataProvider<Integer, OrderMatch> provider) {

        if (OrderType.DEPTHONLY != order.getOrderType()) {
            return false;
        }

        orderArray.findAndRemoveOrder(order.getOrderId());
        orderArray.depthOnlySize -= order.getRemainSize();

        OrderMatch depthOnlyMatch = provider.feedOne(symbolId);
        depthOnlyMatch.from(sequence, symbolId, order, orderArray, timestamp);
        provider.push();
        return true;
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
