package com.github.fevernova.task.marketdepth.engine;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.google.common.collect.Maps;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;


@Getter
public class SymbolDepths implements WriteBytesMarshallable, ReadBytesMarshallable {


    private DepthBooks bids = new DepthBooks(Maps.newTreeMap((l1, l2) -> l2.compareTo(l1)));

    private DepthBooks asks = new DepthBooks(Maps.newTreeMap(Long::compareTo));

    private long lastSequence;

    private boolean update = false;

    private long lastDumpTime = Util.nowMS();


    public void handle(OrderMatch match) {

        if (this.lastSequence >= match.getSequence()) {
            return;
        }
        this.lastSequence = match.getSequence();
        DepthBooks depthBooks = OrderAction.BID == match.getOrderAction() ? this.bids : this.asks;
        depthBooks.handle(match.getOrderPrice(), match.getOrderPriceDepthSize(), match.getOrderPriceOrderCount());
        this.update = true;
    }


    public boolean dump4RealTime() {

        return this.update && Util.nowMS() - this.lastDumpTime >= 1000L;
    }


    public void completedDump() {

        this.update = false;
        this.lastDumpTime = Util.nowMS();
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.bids.readMarshallable(bytes);
        this.asks.readMarshallable(bytes);
        this.lastSequence = bytes.readLong();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        this.bids.writeMarshallable(bytes);
        this.asks.writeMarshallable(bytes);
        bytes.writeLong(this.lastSequence);
    }
}
