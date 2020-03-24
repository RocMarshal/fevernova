package com.github.fevernova.task.exchange.data.order.condition;


import com.github.fevernova.framework.common.structure.queue.LinkedQueue;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


@Getter
public class ConditionOrderArray implements WriteBytesMarshallable {


    private final LinkedQueue<ConditionOrder> queue = new LinkedQueue<>();

    private long price;

    private long size;


    public ConditionOrderArray(BytesIn bytes) {

        this.price = bytes.readLong();
        int length = bytes.readInt();
        for (int i = 0; i < length; i++) {
            ConditionOrder order = new ConditionOrder(bytes);
            this.queue.offer(order);
            this.size += order.getSize();
        }
    }


    public ConditionOrderArray(long price) {

        this.price = price;
    }


    public void addOrder(ConditionOrder order) {

        this.queue.offer(order);
        this.size += order.getSize();
    }


    public ConditionOrder findAndRemoveOrder(long orderId) {

        ConditionOrder r = this.queue.findAndRemove(order -> order.getOrderId() == orderId);
        if (r != null) {
            this.size -= r.getSize();
            r.cancel();
        }
        return r;
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeLong(this.price);
        bytes.writeInt(this.queue.size());
        for (ConditionOrder order : this.queue) {
            order.writeMarshallable(bytes);
        }
    }
}
