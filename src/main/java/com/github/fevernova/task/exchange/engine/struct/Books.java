package com.github.fevernova.task.exchange.engine.struct;


import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.exchange.engine.OrderArray;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;


@Getter
public abstract class Books implements WriteBytesMarshallable, ReadBytesMarshallable {


    protected final TreeMap<Long, OrderArray> priceTree;

    protected OrderAction orderAction;

    protected long price;

    protected long size = 0L;//orderArray的size累加值

    protected OrderArray orderArray;//price对应的OrderArray


    public Books(TreeMap<Long, OrderArray> priceTree, OrderAction orderAction) {

        this.priceTree = priceTree;
        this.orderAction = orderAction;
        this.price = defaultPrice();
    }


    protected abstract long defaultPrice();


    protected abstract boolean newPrice(long tmpPrice);


    public boolean canMatchAll(OrderCommand orderCommand) {

        if (newPrice(orderCommand.getPrice())) {
            return false;
        }
        NavigableMap<Long, OrderArray> subMap = this.priceTree.subMap(this.price, true, orderCommand.getPrice(), true);
        long tmpSize = subMap.entrySet().stream().mapToLong(value -> value.getValue().getSize()).sum();
        return orderCommand.getSize() <= tmpSize;
    }


    public OrderArray getOrCreateOrderArray(OrderCommand orderCommand) {

        if (this.price == orderCommand.getPrice()) {
            return this.orderArray;
        }
        OrderArray oa = this.priceTree.get(orderCommand.getPrice());
        if (oa == null) {
            oa = new OrderArray(this.orderAction, orderCommand.getPrice());
            this.priceTree.put(orderCommand.getPrice(), oa);
            if (newPrice(orderCommand.getPrice())) {
                this.price = orderCommand.getPrice();
                this.orderArray = oa;
            }
        }
        return oa;
    }


    public Order addOrder(OrderCommand orderCommand, OrderArray oa) {

        Order order = new Order(orderCommand);
        oa.addOrder(order);
        this.size += order.getRemainSize();
        return order;
    }


    public void IOCClear(OrderCommand orderCommand, Order order, OrderArray oa, List<OrderMatch> result) {

        if (order.needIOCClear()) {
            oa.removeOrder(order);
            adjustByOrderArray(order.getRemainSize(), oa);
            OrderMatch orderMatch = new OrderMatch();
            orderMatch.from(orderCommand, order);
            result.add(orderMatch);
        }
    }


    public void cancel(OrderCommand orderCommand, OrderMatch orderMatch) {

        OrderArray oa = this.priceTree.get(orderCommand.getPrice());

        if (oa == null) {
            orderMatch.setResultCode(ResultCode.INVALID_CANCEL_NO_ORDER_ID);
            return;
        }
        Order order = oa.findOrder(orderCommand.getOrderId());
        if (order == null) {
            orderMatch.setResultCode(ResultCode.INVALID_CANCEL_NO_ORDER_ID);
            return;
        }
        orderMatch.setResultCode(ResultCode.CANCEL_USER);
        oa.removeOrder(order);
        adjustByOrderArray(order.getRemainSize(), oa);
    }


    public void adjustByOrderArray(long delta, OrderArray oa) {

        this.size -= delta;
        if (oa.getSize() == 0L) {
            this.priceTree.remove(oa.getPrice());
            if (this.price == oa.getPrice()) {
                Map.Entry<Long, OrderArray> tme = this.priceTree.ceilingEntry(this.price);
                this.price = (tme == null ? defaultPrice() : tme.getKey());
                this.orderArray = (tme == null ? null : tme.getValue());
            }
        }
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        int treeSize = bytes.readInt();
        for (int i = 0; i < treeSize; i++) {
            OrderArray orderArray = new OrderArray(bytes);
            this.priceTree.put(orderArray.getPrice(), orderArray);
        }
        this.price = bytes.readLong();
        this.size = bytes.readLong();
        this.orderArray = this.priceTree.get(this.price);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.priceTree.size());
        this.priceTree.values().forEach(orderArray -> orderArray.writeMarshallable(bytes));
        bytes.writeLong(this.price);
        bytes.writeLong(this.size);
    }
}
