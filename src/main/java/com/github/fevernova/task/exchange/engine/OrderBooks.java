package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.Sequence;
import com.github.fevernova.task.exchange.data.candle.Line;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.exchange.data.uniq.UniqIdFilter;
import com.github.fevernova.task.exchange.engine.struct.AskBooks;
import com.github.fevernova.task.exchange.engine.struct.BidBooks;
import com.github.fevernova.task.exchange.engine.struct.Books;
import lombok.Getter;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


@Getter
public final class OrderBooks implements WriteBytesMarshallable {


    private final int symbolId;

    @Setter
    private long lastMatchPrice = 0L;

    private final Sequence sequence = new Sequence();

    private final Books askBooks = new AskBooks();

    private final Books bidBooks = new BidBooks();

    private final UniqIdFilter uniqIdFilter = new UniqIdFilter(60_000L, 10);

    private final Line line = new Line(60_000L, 10);


    public OrderBooks(int symbolId) {

        this.symbolId = symbolId;
    }


    public OrderBooks(BytesIn bytes) {

        this.symbolId = bytes.readInt();
        this.lastMatchPrice = bytes.readLong();
        this.sequence.set(bytes.readLong());
        this.askBooks.readMarshallable(bytes);
        this.bidBooks.readMarshallable(bytes);
        this.uniqIdFilter.readMarshallable(bytes);
        this.line.readMarshallable(bytes);
    }


    public void place(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        if (!this.uniqIdFilter.unique(orderCommand.getTimestamp(), orderCommand.getOrderId())) {
            return;
        }

        Books thisBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.askBooks : this.bidBooks;
        Books thatBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.bidBooks : this.askBooks;

        if (OrderType.FOK == orderCommand.getOrderType() && !thatBooks.canMatchAll(orderCommand)) {
            OrderMatch orderMatch = provider.feedOne(orderCommand.getSymbolId());
            orderMatch.from(this.sequence, orderCommand);
            orderMatch.setResultCode(ResultCode.CANCEL_FOK);
            provider.push();
            return;
        }

        OrderArray orderArray = thisBooks.getOrCreateOrderArray(orderCommand);
        Order order = new Order(orderCommand);
        orderArray.addOrder(order);

        OrderMatch orderPlaceMatch = provider.feedOne(orderCommand.getSymbolId());
        orderPlaceMatch.from(this.sequence, orderCommand, order, orderArray);
        orderPlaceMatch.setResultCode(ResultCode.PLACE);
        provider.push();

        matchOrders(provider, orderCommand.getTimestamp(), orderCommand.getOrderAction());

        if (order.needIOCClear()) {
            orderArray.findAndRemoveOrder(order.getOrderId());
            thisBooks.adjustByOrderArray(orderArray);
            OrderMatch orderMatch = provider.feedOne(orderCommand.getSymbolId());
            orderMatch.from(this.sequence, orderCommand, order, orderArray);
            orderMatch.setResultCode(ResultCode.CANCEL_IOC);
            provider.push();
        }
        thisBooks.handleLazy();
    }


    private void matchOrders(DataProvider<Integer, OrderMatch> provider, long timestamp, OrderAction driverAction) {

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
            long matchSize;
            if (bidOrderArray.getSize() > askOrderArray.getSize()) {
                matchSize = askOrderArray.getSize();
                bidOrderArray.meet(this.sequence, askOrderArray, this.symbolId, this.lastMatchPrice, provider, timestamp, driverAction);
            } else {
                matchSize = bidOrderArray.getSize();
                askOrderArray.meet(this.sequence, bidOrderArray, this.symbolId, this.lastMatchPrice, provider, timestamp, driverAction);
            }
            this.line.acc(timestamp, this.lastMatchPrice, matchSize, this.sequence.get());
            this.bidBooks.adjustByOrderArray(bidOrderArray);
            this.askBooks.adjustByOrderArray(askOrderArray);
        }
    }


    public void cancel(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        Books books = OrderAction.ASK == orderCommand.getOrderAction() ? this.askBooks : this.bidBooks;
        books.cancel(orderCommand, provider, this.sequence);
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        bytes.writeLong(this.lastMatchPrice);
        bytes.writeLong(this.sequence.get());
        this.askBooks.writeMarshallable(bytes);
        this.bidBooks.writeMarshallable(bytes);
        this.uniqIdFilter.writeMarshallable(bytes);
        this.line.writeMarshallable(bytes);
    }
}
