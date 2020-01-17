package com.github.fevernova.task.exchange.data.depth;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.task.exchange.engine.OrderArray;
import com.github.fevernova.task.exchange.engine.OrderBooks;
import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.github.fevernova.task.exchange.engine.struct.Books;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Map;


@Getter
public class DepthResult implements WriteBytesMarshallable {


    private int symbolId;

    private long lastSequence;

    private long lastMatchPrice;

    private long timestamp;

    private long[] bidPrice;

    private long[] bidSize;

    private int[] bidOrderCount;

    private long[] askPrice;

    private long[] askSize;

    private int[] askOrderCount;


    public DepthResult(OrderBooks orderBooks, int maxDepthSize) {

        this.symbolId = orderBooks.getSymbolId();
        this.lastSequence = orderBooks.getSequence().get();
        this.lastMatchPrice = orderBooks.getLastMatchPrice();
        this.timestamp = Util.nowMS();
        Triple<long[], long[], int[]> tpBid = loadData(orderBooks.getBidBooks(), maxDepthSize);
        this.bidPrice = tpBid.getLeft();
        this.bidSize = tpBid.getMiddle();
        this.bidOrderCount = tpBid.getRight();
        Triple<long[], long[], int[]> tpAsk = loadData(orderBooks.getAskBooks(), maxDepthSize);
        this.askPrice = tpAsk.getLeft();
        this.askSize = tpAsk.getMiddle();
        this.askOrderCount = tpAsk.getRight();
    }


    private Triple<long[], long[], int[]> loadData(Books books, int maxDepthSize) {

        int size = Math.min(maxDepthSize, books.getPriceTree().size());
        long[] l1 = new long[size];
        long[] l2 = new long[size];
        int[] l3 = new int[size];
        int cursor = 0;
        for (Map.Entry<Long, OrderArray> entry : books.getPriceTree().entrySet()) {
            if (cursor == size) {
                break;
            }
            l1[cursor] = entry.getKey();
            l2[cursor] = entry.getValue().getSize();
            l3[cursor] = entry.getValue().getQueue().size();
            cursor++;
        }
        return Triple.of(l1, l2, l3);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        bytes.writeLong(this.lastSequence);
        bytes.writeLong(this.lastMatchPrice);
        bytes.writeLong(this.timestamp);
        SerializationUtils.writeLongArray(this.bidPrice, bytes);
        SerializationUtils.writeLongArray(this.bidSize, bytes);
        SerializationUtils.writeIntArray(this.bidOrderCount, bytes);
        SerializationUtils.writeLongArray(this.askPrice, bytes);
        SerializationUtils.writeLongArray(this.askSize, bytes);
        SerializationUtils.writeIntArray(this.askOrderCount, bytes);
    }
}