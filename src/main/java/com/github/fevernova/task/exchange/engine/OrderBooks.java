package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.order.Order;
import com.github.fevernova.task.exchange.data.order.OrderAction;
import com.github.fevernova.task.exchange.data.order.OrderType;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.data.result.ResultCode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;


public final class OrderBooks implements WriteBytesMarshallable {


    private final TreeMap<Long, OrderArray> askPriceTree = Maps.newTreeMap(Long::compareTo);

    private final TreeMap<Long, OrderArray> bidPriceTree = Maps.newTreeMap((l1, l2) -> 0 - l1.compareTo(l2));

    @Getter
    private final int symbolId;

    private long askPrice = Long.MAX_VALUE;//卖的最低价

    private OrderArray askMinOrderArray;

    private long askSize = 0L;//当前卖出量

    private long bidPrice = 0L;//买的最高价

    private OrderArray bidMaxOrderArray;

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

        this.askMinOrderArray = this.askPriceTree.get(this.askPrice);
        this.bidMaxOrderArray = this.bidPriceTree.get(this.bidPrice);
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

        if (OrderType.FOK == orderCommand.getOrderType()) {
            long tmpSize = 0;
            if (orderCommand.getPrice() <= this.bidPrice) {
                NavigableMap<Long, OrderArray> subMap = this.bidPriceTree.subMap(this.bidPrice, true, orderCommand.getPrice(), true);
                tmpSize = subMap.entrySet().stream().mapToLong(value -> value.getValue().getSize()).sum();
            }
            if (orderCommand.getSize() > tmpSize) {
                OrderMatch orderMatch = new OrderMatch();
                orderMatch.from(orderCommand);
                orderMatch.setResultCode(ResultCode.CANCEL_FOK);
                List<OrderMatch> result = Lists.newLinkedList();
                result.add(orderMatch);
                return result;
            }
        }

        OrderArray orderArray;
        if (this.askPrice == orderCommand.getPrice()) {
            orderArray = this.askMinOrderArray;
        } else {
            orderArray = this.askPriceTree.get(orderCommand.getPrice());
            if (orderArray == null) {
                orderArray = new OrderArray(OrderAction.ASK, orderCommand.getPrice());
                this.askPriceTree.put(orderCommand.getPrice(), orderArray);
                if (orderCommand.getPrice() < this.askPrice) {
                    this.askPrice = orderCommand.getPrice();
                    this.askMinOrderArray = orderArray;
                }
            }
        }
        Order order = new Order(orderCommand);
        orderArray.addOrder(order);
        this.askSize += order.getRemainSize();
        List<OrderMatch> result = matchOrders();
        if (OrderType.IOC == orderCommand.getOrderType() && order.getRemainSize() > 0) {
            cancelAsk(order, orderArray);
            OrderMatch orderMatch = new OrderMatch();
            orderMatch.from(orderCommand, order);
            result.add(orderMatch);
        }
        return result;
    }


    private List<OrderMatch> matchBid(OrderCommand orderCommand) {

        if (OrderType.FOK == orderCommand.getOrderType()) {
            long tmpSize = 0;
            if (orderCommand.getPrice() >= this.askPrice) {
                NavigableMap<Long, OrderArray> subMap = this.askPriceTree.subMap(this.askPrice, true, orderCommand.getPrice(), true);
                tmpSize = subMap.entrySet().stream().mapToLong(value -> value.getValue().getSize()).sum();
            }
            if (orderCommand.getSize() > tmpSize) {
                OrderMatch orderMatch = new OrderMatch();
                orderMatch.from(orderCommand);
                orderMatch.setResultCode(ResultCode.CANCEL_FOK);
                List<OrderMatch> result = Lists.newLinkedList();
                result.add(orderMatch);
                return result;
            }
        }

        OrderArray orderArray;
        if (this.bidPrice == orderCommand.getPrice()) {
            orderArray = this.bidMaxOrderArray;
        } else {
            orderArray = this.bidPriceTree.get(orderCommand.getPrice());
            if (orderArray == null) {
                orderArray = new OrderArray(OrderAction.BID, orderCommand.getPrice());
                this.bidPriceTree.put(orderArray.getPrice(), orderArray);
                if (orderCommand.getPrice() > this.bidPrice) {
                    this.bidPrice = orderCommand.getPrice();
                    this.bidMaxOrderArray = orderArray;
                }
            }
        }
        Order order = new Order(orderCommand);
        orderArray.addOrder(order);
        this.bidSize += order.getRemainSize();
        List<OrderMatch> result = matchOrders();
        if (OrderType.IOC == orderCommand.getOrderType() && order.getRemainSize() > 0) {
            cancelBid(order, orderArray);
            OrderMatch orderMatch = new OrderMatch();
            orderMatch.from(orderCommand, order);
            result.add(orderMatch);
        }
        return result;
    }


    private List<OrderMatch> matchOrders() {

        if (this.askPrice > this.bidPrice) {
            return Lists.newLinkedList();
        }
        List<OrderMatch> result = Lists.newLinkedList();
        while (this.askPrice <= this.bidPrice && this.askSize > 0 && this.bidSize > 0) {
            //限价撮合的定价逻辑
            if (this.askPrice == this.bidPrice) {
                this.lastMatchPrice = this.askPrice;
            } else if (this.lastMatchPrice <= this.askPrice) {
                this.lastMatchPrice = this.askPrice;
            } else if (this.lastMatchPrice >= this.bidPrice) {
                this.lastMatchPrice = this.bidPrice;
            }
            long bidTmpSize = this.bidMaxOrderArray.getSize();
            long askTmpSize = this.askMinOrderArray.getSize();
            if (bidTmpSize > askTmpSize) {
                this.bidMaxOrderArray.meet(this.askMinOrderArray, this.symbolId, this.lastMatchPrice, result);
            } else {
                this.askMinOrderArray.meet(this.bidMaxOrderArray, this.symbolId, this.lastMatchPrice, result);
            }
            this.bidSize -= (bidTmpSize - this.bidMaxOrderArray.getSize());
            this.askSize -= (askTmpSize - this.askMinOrderArray.getSize());
            adjustBidOrderArray(this.bidMaxOrderArray);
            adjustAskOrderArray(this.askMinOrderArray);
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
            orderMatch.setResultCode(ResultCode.CANCEL_USER);
            if (OrderAction.ASK == orderCommand.getOrderAction()) {
                cancelAsk(order, orderArray);
            } else {
                cancelBid(order, orderArray);
            }
        }
    }


    private void cancelBid(Order order, OrderArray orderArray) {

        this.bidSize -= order.getRemainSize();
        orderArray.removeOrder(order);
        adjustBidOrderArray(orderArray);
    }


    private void cancelAsk(Order order, OrderArray orderArray) {

        this.askSize -= order.getRemainSize();
        orderArray.removeOrder(order);
        adjustAskOrderArray(orderArray);
    }


    private void adjustBidOrderArray(OrderArray orderArray) {

        if (orderArray.getSize() == 0L) {
            this.bidPriceTree.remove(orderArray.getPrice());
            if (this.bidPrice == orderArray.getPrice()) {
                Map.Entry<Long, OrderArray> tme = this.bidPriceTree.ceilingEntry(this.bidPrice);
                this.bidPrice = (tme == null ? 0L : tme.getKey());
                this.bidMaxOrderArray = (tme == null ? null : tme.getValue());
            }
        }
    }


    private void adjustAskOrderArray(OrderArray orderArray) {

        if (orderArray.getSize() == 0L) {
            this.askPriceTree.remove(orderArray.getPrice());
            if (this.askPrice == orderArray.getPrice()) {
                Map.Entry<Long, OrderArray> tme = this.askPriceTree.ceilingEntry(this.askPrice);
                this.askPrice = (tme == null ? Long.MAX_VALUE : tme.getKey());
                this.askMinOrderArray = (tme == null ? null : tme.getValue());
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
