package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.framework.common.structure.queue.LinkedQueue;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.Sequence;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.condition.ConditionOrder;
import com.github.fevernova.task.exchange.data.condition.ConditionOrderArray;
import com.github.fevernova.task.exchange.data.order.*;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.github.fevernova.task.exchange.data.uniq.UniqIdFilter;
import com.github.fevernova.task.exchange.engine.struct.*;
import lombok.Getter;
import lombok.Setter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;


public final class OrderBooks implements WriteBytesMarshallable {


    @Getter
    private final int symbolId;

    @Setter
    private long lastMatchPrice = 0L;

    private final Sequence sequence = new Sequence();

    @Getter
    private final Books askBooks = new AskBooks();

    @Getter
    private final Books bidBooks = new BidBooks();

    private final UniqIdFilter uniqIdFilter = new UniqIdFilter(60_000L, 5);

    private final ConditionBooks upBooks = new UpConditionBooks();

    private final ConditionBooks downBooks = new DownConditionBooks();


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
        this.upBooks.readMarshallable(bytes);
        this.downBooks.readMarshallable(bytes);
    }


    public void place(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        place(orderCommand, provider, true, null);
    }


    private void place(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider, boolean withUniq, LinkedQueue<ConditionOrder> queue) {

        if (withUniq && !this.uniqIdFilter.unique(orderCommand.getTimestamp(), orderCommand.getOrderId())) {
            return;
        }

        Books thisBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.askBooks : this.bidBooks;
        Books thatBooks = OrderAction.ASK == orderCommand.getOrderAction() ? this.bidBooks : this.askBooks;

        if (OrderType.FOK == orderCommand.getOrderType() && !thatBooks.canMatchAll(orderCommand)) {
            command2result(orderCommand, provider, ResultCode.CANCEL_FOK);
        }

        if (OrderType.POSTONLY == orderCommand.getOrderType() && !thatBooks.newEdgePrice(orderCommand.getPrice())) {
            command2result(orderCommand, provider, ResultCode.CANCEL_POSTONLY);
        }

        Order order = thisBooks.place(orderCommand, provider, this.sequence);
        matchOrders(provider, orderCommand.getTimestamp(), orderCommand.getOrderAction());

        if (order.needIOCClear()) {
            thisBooks.cancel(orderCommand, provider, this.sequence, ResultCode.CANCEL_IOC);
        }

        thisBooks.handleLazy();

        if (queue != null) {
            scanConditionBooks(queue);
        } else {
            convertCondition2Simple(orderCommand.getTimestamp(), provider);
        }
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

            bidOrderArray.meet(this.sequence, askOrderArray, this.symbolId, this.lastMatchPrice, provider, timestamp, driverAction);

            this.bidBooks.adjustByOrderArray(bidOrderArray);
            this.askBooks.adjustByOrderArray(askOrderArray);
        }
    }


    public void cancel(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        Books books = OrderAction.ASK == orderCommand.getOrderAction() ? this.askBooks : this.bidBooks;
        books.cancel(orderCommand, provider, this.sequence, ResultCode.CANCEL);
    }


    public void placeCondition(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        if (!this.uniqIdFilter.unique(orderCommand.getTimestamp(), orderCommand.getOrderId())) {
            return;
        }
        ConditionBooks books = OrderMode.CONDITION_UP == orderCommand.getOrderMode() ? this.upBooks : this.downBooks;
        ConditionOrderArray orderArray = books.getOrCreate(orderCommand);
        ConditionOrder order = new ConditionOrder(orderCommand);
        orderArray.addOrder(order);

        OrderMatch orderPlaceMatch = provider.feedOne(orderCommand.getSymbolId());
        orderPlaceMatch.from(this.sequence, orderCommand, order);
        orderPlaceMatch.setResultCode(ResultCode.PLACE);
        provider.push();

        convertCondition2Simple(orderCommand.getTimestamp(), provider);
    }


    private void convertCondition2Simple(long timestamp, DataProvider<Integer, OrderMatch> provider) {

        LinkedQueue<ConditionOrder> queue = scanConditionBooks(null);
        ConditionOrder tmp = queue.poll();
        while (tmp != null) {
            OrderCommand cmd = new OrderCommand();
            cmd.from(this.symbolId, tmp, timestamp);
            place(cmd, provider, false, queue);
            tmp = queue.poll();
        }
    }


    private LinkedQueue<ConditionOrder> scanConditionBooks(LinkedQueue<ConditionOrder> queue) {

        if (queue == null) {
            queue = new LinkedQueue<>();
        }
        while (!this.upBooks.newEdgePrice(this.lastMatchPrice)) {
            ConditionOrderArray orderArray = this.upBooks.getOrderArray();
            if (orderArray == null) {
                break;
            }
            ConditionOrder tmp = orderArray.getQueue().poll();
            while (tmp != null) {
                queue.offer(tmp);
                tmp = orderArray.getQueue().poll();
            }
            this.upBooks.adjust(orderArray, true);
        }
        while (!this.downBooks.newEdgePrice(this.lastMatchPrice)) {
            ConditionOrderArray orderArray = this.downBooks.getOrderArray();
            if (orderArray == null) {
                break;
            }
            ConditionOrder tmp = orderArray.getQueue().poll();
            while (tmp != null) {
                queue.offer(tmp);
                tmp = orderArray.getQueue().poll();
            }
            this.downBooks.adjust(orderArray, true);
        }
        return queue;
    }


    public void cancelCondition(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        ConditionBooks books = OrderMode.CONDITION_UP == orderCommand.getOrderMode() ? this.upBooks : this.downBooks;
        books.cancel(orderCommand, provider, this.sequence);
    }


    public void command2result(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider, ResultCode resultCode) {

        OrderMatch orderMatch = provider.feedOne(orderCommand.getSymbolId());
        orderMatch.from(this.sequence, orderCommand);
        orderMatch.setResultCode(resultCode);
        provider.push();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        bytes.writeLong(this.lastMatchPrice);
        bytes.writeLong(this.sequence.get());
        this.askBooks.writeMarshallable(bytes);
        this.bidBooks.writeMarshallable(bytes);
        this.uniqIdFilter.writeMarshallable(bytes);
        this.upBooks.writeMarshallable(bytes);
        this.downBooks.writeMarshallable(bytes);
    }
}
