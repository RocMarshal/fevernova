package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;
import java.util.TreeMap;


public class OrderBooks implements WriteBytesMarshallable {


    private final TreeMap<Long, OrderArray> askPriceTree = Maps.newTreeMap(Long::compareTo);

    private final TreeMap<Long, OrderArray> bidPriceTree = Maps.newTreeMap((l1, l2) -> 0 - l1.compareTo(l2));

    private final int symbolId;

    private long askPrice = Long.MAX_VALUE;//卖的最低价

    private long askSize = 0L;//当前卖出量

    private long bidPrice = 0L;//买的最高价

    private long bidSize = 0L;//当前买入量

    private long lastMatchPrice = 0;


    public OrderBooks(int symbolId) {

        this.symbolId = symbolId;
    }


    public OrderBooks(BytesIn bytes) {

        this.symbolId = bytes.readInt();
        this.askPrice = bytes.readLong();
        this.askSize = bytes.readLong();
        this.bidPrice = bytes.readLong();
        this.bidSize = bytes.readLong();
        this.lastMatchPrice = bytes.readLong();

        loadPriceTree(bytes, this.askPriceTree);
        loadPriceTree(bytes, this.bidPriceTree);
    }


    private void loadPriceTree(BytesIn bytes, TreeMap<Long, OrderArray> priceTree) {

        int treeSize = bytes.readInt();
        for (int i = 0; i < treeSize; i++) {
            OrderArray orderArray = new OrderArray(bytes);
            priceTree.put(orderArray.getPrice(), orderArray);
        }
    }


    public List<OrderMatch> match(OrderCommand orderCommand) {

        if (OrderAction.ASK == orderCommand.getOrderAction()) {
            return matchAsk(orderCommand);
        } else {
            return matchBid(orderCommand);
        }
    }


    private List<OrderMatch> matchAsk(OrderCommand orderCommand) {

        Order order = new Order(orderCommand);
        OrderArray orderArray = getOrderArray(orderCommand.getPrice(), OrderAction.ASK, this.askPriceTree);
        orderArray.addOrder(order);
        this.askPrice = Math.min(this.askPrice, orderArray.getPrice());
        this.askSize += order.getRemainSize();
        List<OrderMatch> result = matchOrders();
        if (OrderType.IOC == orderCommand.getOrderType() && order.getRemainSize() > 0) {
            cancelAsk(order);
            OrderMatch orderMatch = new OrderMatch();
            orderMatch.from(orderCommand);
            orderMatch.setVersion(order.getVersion());
            orderMatch.setResultCode(ResultCode.CANCEL_IOC);
            result.add(orderMatch);
        }
        return result;
    }


    private List<OrderMatch> matchBid(OrderCommand orderCommand) {

        Order order = new Order(orderCommand);
        OrderArray orderArray = getOrderArray(orderCommand.getPrice(), OrderAction.BID, this.bidPriceTree);
        orderArray.addOrder(order);
        this.bidPrice = Math.max(this.bidPrice, orderArray.getPrice());
        this.bidSize += order.getRemainSize();
        List<OrderMatch> result = matchOrders();
        if (OrderType.IOC == orderCommand.getOrderType() && order.getRemainSize() > 0) {
            cancelBid(order);
            OrderMatch orderMatch = new OrderMatch();
            orderMatch.from(orderCommand);
            orderMatch.setVersion(order.getVersion());
            orderMatch.setResultCode(ResultCode.CANCEL_IOC);
            result.add(orderMatch);
        }
        return result;
    }


    private static OrderArray getOrderArray(long price, OrderAction orderAction, TreeMap<Long, OrderArray> priceTree) {

        OrderArray orderArray = priceTree.get(price);
        if (orderArray == null) {
            orderArray = new OrderArray(orderAction, price);
            priceTree.put(price, orderArray);
        }
        return orderArray;
    }


    private List<OrderMatch> matchOrders() {

        if (this.askPrice > this.bidPrice) {
            return Lists.newLinkedList();
        }
        List<OrderMatch> result = Lists.newLinkedList();
        while (this.askPrice <= this.bidPrice && this.askSize > 0 && this.bidSize > 0) {
            OrderArray bid = this.bidPriceTree.firstEntry().getValue();
            long bidTmpSize = bid.getSize();
            OrderArray ask = this.askPriceTree.firstEntry().getValue();
            long askTmpSize = ask.getSize();

            //限价撮合的定价逻辑
            if (this.askPrice == this.bidPrice) {
                this.lastMatchPrice = this.askPrice;
            } else if (this.lastMatchPrice <= this.askPrice) {
                this.lastMatchPrice = this.askPrice;
            } else if (this.lastMatchPrice >= this.bidPrice) {
                this.lastMatchPrice = this.bidPrice;
            }

            if (bidTmpSize > askTmpSize) {
                result.addAll(bid.meet(ask, this.symbolId, this.lastMatchPrice));
            } else {
                result.addAll(ask.meet(bid, this.symbolId, this.lastMatchPrice));
            }
            adjustBidOrderArray(bid);
            adjustAskOrderArray(ask);
            this.bidSize -= (bidTmpSize - bid.getSize());
            this.askSize -= (askTmpSize - ask.getSize());
        }
        return result;
    }


    public void cancel(OrderCommand orderCommand, OrderMatch orderMatch) {

        OrderArray orderArray = (OrderAction.ASK == orderCommand.getOrderAction() ? this.askPriceTree.get(orderCommand.getPrice()) :
                this.bidPriceTree.get(orderCommand.getPrice()));
        if (orderArray == null) {
            orderMatch.setResultCode(ResultCode.INVALID_CANCEL_NO_ORDER_ID);
            return;
        }
        Order order = orderArray.findOrder(orderCommand.getOrderId());
        if (order == null) {
            orderMatch.setResultCode(ResultCode.INVALID_CANCEL_NO_ORDER_ID);
            return;
        } else {
            orderMatch.setResultCode(ResultCode.CANCEL);
            if (OrderAction.ASK == orderCommand.getOrderAction()) {
                cancelAsk(order);
            } else {
                cancelBid(order);
            }
        }
    }


    private void cancelBid(Order order) {

        this.bidSize -= order.getRemainSize();
        OrderArray orderArray = order.getLink();
        orderArray.remove(order);
        adjustBidOrderArray(orderArray);
    }


    private void cancelAsk(Order order) {

        this.askSize -= order.getRemainSize();
        OrderArray orderArray = order.getLink();
        orderArray.remove(order);
        adjustAskOrderArray(orderArray);
    }


    private void adjustBidOrderArray(OrderArray orderArray) {

        if (orderArray.getSize() == 0L) {
            this.bidPriceTree.remove(orderArray.getPrice());
            if (this.bidPrice == orderArray.getPrice()) {
                Long newPrice = this.bidPriceTree.ceilingKey(this.bidPrice);
                this.bidPrice = (newPrice == null ? 0L : newPrice);
            }
        }
    }


    private void adjustAskOrderArray(OrderArray orderArray) {

        if (orderArray.getSize() == 0L) {
            this.askPriceTree.remove(orderArray.getPrice());
            if (this.askPrice == orderArray.getPrice()) {
                Long newPrice = this.askPriceTree.ceilingKey(this.askPrice);
                this.askPrice = (newPrice == null ? Long.MAX_VALUE : newPrice);
            }
        }
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.symbolId);
        bytes.writeLong(this.askPrice);
        bytes.writeLong(this.askSize);
        bytes.writeLong(this.bidPrice);
        bytes.writeLong(this.bidSize);
        bytes.writeLong(this.lastMatchPrice);

        bytes.writeInt(this.askPriceTree.size());
        this.askPriceTree.values().forEach(orderArray -> orderArray.writeMarshallable(bytes));
        bytes.writeInt(this.bidPriceTree.size());
        this.bidPriceTree.values().forEach(orderArray -> orderArray.writeMarshallable(bytes));
    }
}
