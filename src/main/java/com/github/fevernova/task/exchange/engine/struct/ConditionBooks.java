package com.github.fevernova.task.exchange.engine.struct;


import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.Sequence;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.condition.ConditionOrder;
import com.github.fevernova.task.exchange.data.condition.ConditionOrderArray;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;

import java.util.Map;
import java.util.NavigableMap;


public abstract class ConditionBooks implements WriteBytesMarshallable, ReadBytesMarshallable {


    protected final NavigableMap<Long, ConditionOrderArray> priceTree;

    protected long price;

    @Getter
    protected ConditionOrderArray orderArray;//price对应的ConditionOrderArray


    public ConditionBooks(NavigableMap<Long, ConditionOrderArray> priceTree) {

        this.priceTree = priceTree;
        this.price = defaultPrice();
    }


    protected abstract long defaultPrice();

    public abstract boolean newEdgePrice(long tmpPrice);


    public ConditionOrder place(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider, Sequence sequence) {

        ConditionOrderArray orderArray = getOrCreate(orderCommand);
        ConditionOrder order = new ConditionOrder(orderCommand);
        orderArray.addOrder(order);

        OrderMatch orderMatch = provider.feedOne(orderCommand.getSymbolId());
        orderMatch.from(sequence, orderCommand, order);
        orderMatch.setResultCode(ResultCode.PLACE);
        provider.push();
        return order;
    }


    private ConditionOrderArray getOrCreate(OrderCommand orderCommand) {

        if (this.price == orderCommand.getTriggerPrice()) {
            return this.orderArray;
        }
        if (newEdgePrice(orderCommand.getTriggerPrice())) {
            ConditionOrderArray oa = new ConditionOrderArray(orderCommand.getTriggerPrice());
            this.priceTree.put(oa.getPrice(), oa);
            this.price = oa.getPrice();
            this.orderArray = oa;
            return oa;
        } else {
            ConditionOrderArray oa = this.priceTree.get(orderCommand.getTriggerPrice());
            if (oa == null) {
                oa = new ConditionOrderArray(orderCommand.getTriggerPrice());
                this.priceTree.put(oa.getPrice(), oa);
            }
            return oa;
        }
    }


    public void cancel(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider, Sequence sequence) {

        ConditionOrderArray oa = this.price == orderCommand.getTriggerPrice() ? this.orderArray : this.priceTree.get(orderCommand.getTriggerPrice());
        if (oa == null) {
            return;
        }
        ConditionOrder order = oa.findAndRemoveOrder(orderCommand.getOrderId());
        if (order == null) {
            return;
        }
        OrderMatch orderMatch = provider.feedOne(orderCommand.getSymbolId());
        orderMatch.from(sequence, orderCommand, order);
        orderMatch.setResultCode(ResultCode.CANCEL);
        provider.push();
        this.adjust(oa, false);
    }


    public void adjust(ConditionOrderArray oa, boolean force) {

        if (oa.getSize() == 0L || force) {
            this.priceTree.remove(oa.getPrice());
            if (this.price == oa.getPrice()) {
                Map.Entry<Long, ConditionOrderArray> tme = this.priceTree.ceilingEntry(this.price);
                this.price = (tme == null ? defaultPrice() : tme.getKey());
                this.orderArray = (tme == null ? null : tme.getValue());
            }
        }
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        int treeSize = bytes.readInt();
        for (int i = 0; i < treeSize; i++) {
            ConditionOrderArray orderArray = new ConditionOrderArray(bytes);
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
