package com.github.fevernova.task.exchange.engine;


import com.github.fevernova.framework.common.context.ContextObject;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.candle.CandleData;
import com.github.fevernova.task.exchange.data.cmd.OrderCommand;
import com.github.fevernova.task.exchange.data.depth.DepthData;
import com.github.fevernova.task.exchange.data.depth.DepthResult;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.lang3.Validate;

import java.util.Map;


public final class OrderBooksEngine extends ContextObject implements WriteBytesMarshallable, ReadBytesMarshallable {


    public static final String CONS_NAME = "OrderBooksEngine";

    private Map<Integer, OrderBooks> symbols;

    private OrderBooks lastOrderBooks;


    public OrderBooksEngine(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
        this.symbols = Maps.newHashMap();
    }


    public void placeOrder(OrderCommand orderCommand, DataProvider<Long, OrderMatch> provider) {

        OrderBooks orderBooks = getOrderBooks(orderCommand);
        orderBooks.place(orderCommand, provider);
    }


    public void cancelOrder(OrderCommand orderCommand, DataProvider<Long, OrderMatch> provider) {

        OrderBooks orderBooks = getOrderBooks(orderCommand);
        orderBooks.cancel(orderCommand, provider);
    }


    private OrderBooks getOrderBooks(OrderCommand orderCommand) {

        int symbolId = orderCommand.getSymbolId();

        if (this.lastOrderBooks != null && this.lastOrderBooks.getSymbolId() == symbolId) {
            return this.lastOrderBooks;
        }
        OrderBooks orderBooks = this.symbols.get(symbolId);
        if (orderBooks == null) {
            orderBooks = new OrderBooks(symbolId);
            this.symbols.put(symbolId, orderBooks);
            this.lastOrderBooks = orderBooks;
        }
        return orderBooks;
    }


    public DepthData dumpDepth() {

        final DepthData depthData = new DepthData(this.symbols.size());
        this.symbols.entrySet().forEach(entry -> depthData.getData().put(entry.getKey(), new DepthResult(entry.getValue(), 500)));
        return depthData;
    }


    public CandleData dumpCandle() {

        final CandleData candleData = new CandleData(this.symbols.size());
        this.symbols.entrySet().forEach(entry -> candleData.getData().put(entry.getKey(), entry.getValue().getLine()));
        return candleData;
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        Validate.isTrue(bytes.readInt() == 0);
        this.symbols = SerializationUtils.readIntHashMap(bytes, bytesIn -> new OrderBooks(bytesIn));
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(0);
        SerializationUtils.writeIntHashMap(this.symbols, bytes);
    }
}
