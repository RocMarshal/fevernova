package com.github.fevernova.task.exchange.engine.struct;


import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.exchange.engine.OrderArray;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.util.Map;
import java.util.NavigableMap;


@Getter
public abstract class Books implements WriteBytesMarshallable, ReadBytesMarshallable {


    protected final NavigableMap<Long, OrderArray> priceTree;

    protected long price;

    protected OrderArray orderArray;//price对应的OrderArray


    public Books(NavigableMap<Long, OrderArray> priceTree) {

        this.priceTree = priceTree;
        this.price = defaultPrice();
    }


    protected abstract long defaultPrice();


    public abstract boolean newEdgePrice(long tmpPrice);


    public boolean canMatchAll(OrderCommand orderCommand) {

        if (newEdgePrice(orderCommand.getPrice())) {
            return false;
        }
        NavigableMap<Long, OrderArray> subMap = this.priceTree.subMap(this.price, true, orderCommand.getPrice(), true);
        long acc = 0L;
        for (Map.Entry<Long, OrderArray> entry : subMap.entrySet()) {
            acc += entry.getValue().getSize();
            if (acc >= orderCommand.getSize()) {
                return true;
            }
        }
        return false;
    }


    public OrderArray getOrCreateOrderArray(OrderCommand orderCommand) {

        if (this.price == orderCommand.getPrice()) {
            return this.orderArray;
        }

        if (newEdgePrice(orderCommand.getPrice())) {
            OrderArray oa = new OrderArray(orderCommand.getOrderAction(), orderCommand.getPrice());
            this.priceTree.put(oa.getPrice(), oa);
            this.price = oa.getPrice();
            this.orderArray = oa;
            return oa;
        } else {
            OrderArray oa = this.priceTree.get(orderCommand.getPrice());
            if (oa == null) {
                oa = new OrderArray(orderCommand.getOrderAction(), orderCommand.getPrice());
                this.priceTree.put(oa.getPrice(), oa);
            }
            return oa;
        }
    }


    public void cancel(OrderCommand orderCommand, DataProvider<Long, OrderMatch> provider) {

        OrderMatch orderMatch = provider.feedOne(orderCommand.getOrderId());
        OrderArray oa = this.priceTree.get(orderCommand.getPrice());
        if (oa == null) {
            orderMatch.from(orderCommand);
            orderMatch.setResultCode(ResultCode.INVALID_CANCEL_NO_ORDER_ID);
            provider.push();
            return;
        }
        Order order = oa.findAndRemoveOrder(orderCommand.getOrderId());
        if (order == null) {
            orderMatch.from(orderCommand);
            orderMatch.setResultCode(ResultCode.INVALID_CANCEL_NO_ORDER_ID);
            provider.push();
            return;
        }
        orderMatch.from(orderCommand, order);
        orderMatch.setResultCode(ResultCode.CANCEL_USER);
        provider.push();
        adjustByOrderArray(oa);
    }


    public void adjustByOrderArray(OrderArray oa) {

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
        this.orderArray = this.priceTree.get(this.price);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.priceTree.size());
        this.priceTree.values().forEach(orderArray -> orderArray.writeMarshallable(bytes));
        bytes.writeLong(this.price);
    }
}
