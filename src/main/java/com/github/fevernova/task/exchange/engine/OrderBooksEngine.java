package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.framework.common.context.ContextObject;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.uniq.SerializationUtils;
import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Map;


public final class OrderBooksEngine extends ContextObject implements WriteBytesMarshallable, ReadBytesMarshallable {


    public static final String CONS_NAME = "OrderBooksEngine";

    private IntObjectHashMap<OrderBooks> symbols;

    private Map<Integer, OrderBooks> symbolsCache;

    private OrderBooks lastOrderBooks;


    public OrderBooksEngine(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
        this.symbols = new IntObjectHashMap<>();
        this.symbolsCache = Maps.newHashMap();
    }


    public void placeOrder(OrderCommand orderCommand, DataProvider<Integer, OrderMatch> provider) {

        OrderBooks orderBooks = getOrderBooks(orderCommand);
        orderBooks.match(orderCommand, provider);
    }


    public void cancelOrder(OrderCommand orderCommand, OrderMatch orderMatch) {

        OrderBooks orderBooks = getOrderBooks(orderCommand);
        orderBooks.cancel(orderCommand, orderMatch);
    }


    private OrderBooks getOrderBooks(OrderCommand orderCommand) {

        int symbolId = orderCommand.getSymbolId();

        if (this.lastOrderBooks != null && this.lastOrderBooks.getSymbolId() == symbolId) {
            return this.lastOrderBooks;
        }
        OrderBooks orderBooks = this.symbolsCache.get(symbolId);
        if (orderBooks == null) {
            orderBooks = new OrderBooks(symbolId);
            this.symbols.put(symbolId, orderBooks);
            this.symbolsCache.put(symbolId, orderBooks);
            this.lastOrderBooks = orderBooks;
        }
        return orderBooks;
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.symbols = SerializationUtils.readIntMap(bytes, bytesIn -> new OrderBooks(bytesIn));
        this.symbols.forEachKeyValue((each, parameter) -> symbolsCache.put(each, parameter));
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        SerializationUtils.marshallIntMap(this.symbols, bytes);
    }
}
