package com.github.fevernova.task.exchange.data.depth;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.task.exchange.engine.OrderArray;
import com.github.fevernova.task.exchange.engine.OrderBooks;
import com.github.fevernova.task.exchange.engine.struct.Books;
import lombok.Getter;

import java.util.Map;


@Getter
public class DepthResult {


    private int symbolId;

    private long lastSequence;

    private long lastMatchPrice;

    private long timestamp;

    private long[][] bids = new long[2][];

    private long[][] asks = new long[2][];


    public DepthResult(OrderBooks orderBooks, int maxDepthSize) {

        this.symbolId = orderBooks.getSymbolId();
        this.lastSequence = orderBooks.getSequence().get();
        this.lastMatchPrice = orderBooks.getLastMatchPrice();
        this.timestamp = Util.nowMS();
        loadData(orderBooks.getBidBooks(), maxDepthSize, this.bids);
        loadData(orderBooks.getAskBooks(), maxDepthSize, this.asks);
    }


    private void loadData(Books books, int maxDepthSize, long[][] data) {

        int size = Math.min(maxDepthSize, books.getPriceTree().size());
        data[0] = new long[size];
        data[1] = new long[size];
        int cursor = 0;
        for (Map.Entry<Long, OrderArray> entry : books.getPriceTree().entrySet()) {
            if (cursor == size) {
                break;
            }
            data[0][cursor] = entry.getKey();
            data[1][cursor] = entry.getValue().getSize();
            cursor++;
        }
    }
}
