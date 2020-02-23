package com.github.fevernova.task.marketdepth.engine;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.marketdepth.books.AskDepthBooks;
import com.github.fevernova.task.marketdepth.books.BidDepthBooks;
import com.github.fevernova.task.marketdepth.books.DepthBooks;
import com.github.fevernova.task.marketdepth.data.DepthResult;
import lombok.Getter;
import lombok.NoArgsConstructor;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;


@NoArgsConstructor
@Getter
public class SymbolDepths implements WriteBytesMarshallable, ReadBytesMarshallable {


    private int symbolId;

    private int maxDepthSize;

    private DepthBooks bids = new BidDepthBooks();

    private DepthBooks asks = new AskDepthBooks();

    private long lastSequence;

    private boolean update = false;

    private long lastDumpTime = Util.nowMS();


    public SymbolDepths(int symbolId, int maxDepthSize) {

        this.symbolId = symbolId;
        this.maxDepthSize = maxDepthSize;
    }


    public void handle(OrderMatch match, DataProvider<Integer, DepthResult> provider, long now) {

        if (this.lastSequence >= match.getSequence()) {
            return;
        }
        this.lastSequence = match.getSequence();
        DepthBooks depthBooks = OrderAction.BID == match.getOrderAction() ? this.bids : this.asks;
        depthBooks.handle(match.getOrderPrice(), match.getOrderPriceDepthSize(), match.getOrderPriceOrderCount());
        this.update = true;
        scan(provider, now);
    }


    public void scan(DataProvider<Integer, DepthResult> provider, long now) {

        if (this.bids.newEdgePrice(this.asks.getCachePrice()) && needDump(now)) {
            dump(provider, now);
        }
    }


    private boolean needDump(long now) {

        if (now - this.lastDumpTime >= 60 * 1000L) {
            return true;
        } else if (this.update && now - this.lastDumpTime >= 1000L) {
            return true;
        }
        return false;
    }


    private void dump(DataProvider<Integer, DepthResult> provider, long now) {

        DepthResult depthResult = provider.feedOne(this.symbolId);
        depthResult.setSymbolId(this.symbolId);
        depthResult.setTimestamp(now);
        depthResult.dump(this, this.maxDepthSize);
        provider.push();
        this.update = false;
        this.lastDumpTime = now;
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.symbolId = bytes.readInt();
        this.maxDepthSize = bytes.readInt();
        this.bids.readMarshallable(bytes);
        this.asks.readMarshallable(bytes);
        this.lastSequence = bytes.readLong();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        bytes.writeInt(this.maxDepthSize);
        this.bids.writeMarshallable(bytes);
        this.asks.writeMarshallable(bytes);
        bytes.writeLong(this.lastSequence);
    }
}
