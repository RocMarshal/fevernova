package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.exchange.engine.struct.AskBooks;
import com.github.fevernova.task.exchange.engine.struct.BidBooks;
import com.github.fevernova.task.exchange.engine.struct.Books;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


public final class OrderBooks implements WriteBytesMarshallable {


    @Getter
    private final int symbolId;

    private long lastMatchPrice = 0;

    private final Books askBooks = new AskBooks();

    private final Books bidBooks = new BidBooks();


    public OrderBooks(int symbolId) {

        this.symbolId = symbolId;
    }


    public OrderBooks(BytesIn bytes) {

        this.symbolId = bytes.readInt();
        this.lastMatchPrice = bytes.readLong();
        this.askBooks.readMarshallable(bytes);
        this.bidBooks.readMarshallable(bytes);
    }


    public void match(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        Books thisBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.askBooks : this.bidBooks;
        Books thatBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.bidBooks : this.askBooks;

        if (OrderType.FOK == orderCommand.getOrderType() && !thatBooks.canMatchAll(orderCommand)) {
            OrderMatch orderMatch = provider.feedOne(orderCommand.getSymbolId());
            orderMatch.from(orderCommand);
            orderMatch.setResultCode(ResultCode.CANCEL_FOK);
            provider.push();
            return;
        }

        OrderArray orderArray = thisBooks.getOrCreateOrderArray(orderCommand);
        Order order = new Order(orderCommand);
        orderArray.addOrder(order);
        matchOrders(provider);

        if (order.needIOCClear()) {
            orderArray.removeOrder(order);
            thisBooks.adjustByOrderArray(orderArray);
            OrderMatch orderMatch = provider.feedOne(orderCommand.getSymbolId());
            orderMatch.from(orderCommand, order);
            provider.push();
        }
    }


    private void matchOrders(DataProvider<Integer, OrderMatch> provider) {

        while (!this.askBooks.newEdgePrice(this.bidBooks.getPrice())) {
            //限价撮合的定价逻辑
            if (this.askBooks.getPrice() == this.bidBooks.getPrice()) {
                this.lastMatchPrice = this.askBooks.getPrice();
            } else if (this.lastMatchPrice <= this.askBooks.getPrice()) {
                this.lastMatchPrice = this.askBooks.getPrice();
            } else if (this.lastMatchPrice >= this.bidBooks.getPrice()) {
                this.lastMatchPrice = this.bidBooks.getPrice();
            }
            OrderArray bidOrderArray = this.bidBooks.getOrderArray();
            OrderArray askOrderArray = this.askBooks.getOrderArray();
            if (bidOrderArray.getSize() > askOrderArray.getSize()) {
                bidOrderArray.meet(askOrderArray, this.symbolId, this.lastMatchPrice, provider);
            } else {
                askOrderArray.meet(bidOrderArray, this.symbolId, this.lastMatchPrice, provider);
            }
            this.bidBooks.adjustByOrderArray(bidOrderArray);
            this.askBooks.adjustByOrderArray(askOrderArray);
        }
    }


    public void cancel(OrderCommand orderCommand, OrderMatch orderMatch) {

        Books books = OrderAction.ASK == orderCommand.getOrderAction() ? this.askBooks : this.bidBooks;
        books.cancel(orderCommand, orderMatch);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        bytes.writeLong(this.lastMatchPrice);
        this.askBooks.writeMarshallable(bytes);
        this.bidBooks.writeMarshallable(bytes);
    }
}
