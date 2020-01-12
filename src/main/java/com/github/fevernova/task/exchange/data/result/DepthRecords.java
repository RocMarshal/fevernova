package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.task.exchange.engine.OrderArray;
import com.github.fevernova.task.exchange.engine.OrderBooks;
import lombok.Getter;

import java.util.Map;


@Getter
public class DepthRecords {


    private int symbolId;

    private long lastSequence;

    private long lastMatchPrice;

    private long[] bidsPrice;

    private long[] bidsSize;

    private long[] asksPrice;

    private long[] asksSize;

    private transient int bidCursor;

    private transient int askCursor;


    public DepthRecords(OrderBooks orderBooks, int maxDepthSize) {

        this.symbolId = orderBooks.getSymbolId();
        this.lastSequence = orderBooks.getSequence().get();
        this.lastMatchPrice = orderBooks.getLastMatchPrice();
        int bidSize = Math.min(maxDepthSize, orderBooks.getBidBooks().getPriceTree().size());
        int askSize = Math.min(maxDepthSize, orderBooks.getAskBooks().getPriceTree().size());
        this.bidsPrice = new long[bidSize];
        this.bidsSize = new long[bidSize];
        this.asksPrice = new long[askSize];
        this.asksSize = new long[askSize];

        for (Map.Entry<Long, OrderArray> entry : orderBooks.getBidBooks().getPriceTree().entrySet()) {
            if (this.bidCursor == bidSize) {
                break;
            }
            put(0, entry.getKey(), entry.getValue().getSize());
        }
        for (Map.Entry<Long, OrderArray> entry : orderBooks.getAskBooks().getPriceTree().entrySet()) {
            if (this.askCursor == askSize) {
                break;
            }
            put(1, entry.getKey(), entry.getValue().getSize());
        }
    }


    public void put(int type, long price, long size) {

        if (type == 0) {
            this.bidsPrice[this.bidCursor] = price;
            this.bidsSize[this.bidCursor] = size;
            this.bidCursor++;
        } else {
            this.asksPrice[this.askCursor] = price;
            this.asksSize[this.askCursor] = size;
            this.askCursor++;
        }
    }
}
