package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.exchange.engine.struct.AskBooks;
import com.github.fevernova.task.exchange.engine.struct.BidBooks;
import com.github.fevernova.task.exchange.engine.struct.Books;
import com.google.common.collect.Lists;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;


public final class OrderBooks implements WriteBytesMarshallable {


    @Getter
    private final int symbolId;

    private long lastMatchPrice = 0;

    private final AskBooks askBooks = new AskBooks();

    private final BidBooks bidBooks = new BidBooks();


    public OrderBooks(int symbolId) {

        this.symbolId = symbolId;
    }


    public OrderBooks(BytesIn bytes) {

        this.symbolId = bytes.readInt();
        this.lastMatchPrice = bytes.readLong();
        this.askBooks.readMarshallable(bytes);
        this.bidBooks.readMarshallable(bytes);
    }


    public List<OrderMatch> match(OrderCommand orderCommand) {

        Books thisBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.askBooks : this.bidBooks;
        Books thatBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.bidBooks : this.askBooks;

        if (OrderType.FOK == orderCommand.getOrderType() && !thatBooks.canMatchAll(orderCommand)) {
            OrderMatch orderMatch = new OrderMatch();
            orderMatch.from(orderCommand);
            orderMatch.setResultCode(ResultCode.CANCEL_FOK);
            List<OrderMatch> result = Lists.newLinkedList();
            result.add(orderMatch);
            return result;
        }

        OrderArray orderArray = thisBooks.getOrCreateOrderArray(orderCommand);
        Order order = thisBooks.addOrder(orderCommand, orderArray);
        List<OrderMatch> result = matchOrders();
        thisBooks.IOCClear(orderCommand, order, orderArray, result);
        return result;
    }


    private List<OrderMatch> matchOrders() {

        List<OrderMatch> result = Lists.newLinkedList();
        if (this.askBooks.getPrice() > this.bidBooks.getPrice()) {
            return result;
        }
        while (this.askBooks.getPrice() <= this.bidBooks.getPrice() && this.askBooks.getSize() > 0 && this.bidBooks.getSize() > 0) {
            //限价撮合的定价逻辑
            if (this.askBooks.getPrice() == this.bidBooks.getPrice()) {
                this.lastMatchPrice = this.askBooks.getPrice();
            } else if (this.lastMatchPrice <= this.askBooks.getPrice()) {
                this.lastMatchPrice = this.askBooks.getPrice();
            } else if (this.lastMatchPrice >= this.bidBooks.getPrice()) {
                this.lastMatchPrice = this.bidBooks.getPrice();
            }
            long bidTmpSize = this.bidBooks.getOrderArray().getSize();
            long askTmpSize = this.askBooks.getOrderArray().getSize();
            if (bidTmpSize > askTmpSize) {
                this.bidBooks.getOrderArray().meet(this.askBooks.getOrderArray(), this.symbolId, this.lastMatchPrice, result);
            } else {
                this.askBooks.getOrderArray().meet(this.bidBooks.getOrderArray(), this.symbolId, this.lastMatchPrice, result);
            }
            this.bidBooks.adjustByOrderArray(bidTmpSize - this.bidBooks.getOrderArray().getSize(), this.bidBooks.getOrderArray());
            this.askBooks.adjustByOrderArray(askTmpSize - this.askBooks.getOrderArray().getSize(), this.askBooks.getOrderArray());
        }
        return result;
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
