package com.github.fevernova.task.marketdepth.engine;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.task.exchange.data.result.OrderMatch;
import com.github.fevernova.task.exchange.engine.SerializationUtils;
import com.github.fevernova.task.marketdepth.data.DepthResult;
import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.lang3.Validate;

import java.util.Map;


public class DepthEngine implements WriteBytesMarshallable, ReadBytesMarshallable {


    public static final String CONS_NAME = "DepthData";

    private Map<Integer, SymbolDepths> data = Maps.newHashMap();

    private int maxDepthSize;

    private long interval;

    private long lastScanTime = Util.nowMS();


    public DepthEngine(int maxDepthSize, long interval) {

        this.maxDepthSize = maxDepthSize;
        this.interval = interval;
    }


    public void handle(OrderMatch match) {

        SymbolDepths symbolDepths = this.data.get(match.getSymbolId());
        if (symbolDepths == null) {
            symbolDepths = new SymbolDepths();
            this.data.put(match.getSymbolId(), symbolDepths);
        }
        symbolDepths.handle(match);
    }


    public void scan(DataProvider<Integer, DepthResult> provider) {

        long now = Util.nowMS();
        if (now - this.lastScanTime >= this.interval) {
            this.lastScanTime = now;
        }
        this.data.forEach((id, symbolDepths) -> {

            if (symbolDepths.dump4RealTime()) {
                dump(id, symbolDepths, provider);
            }
        });
    }


    public void forceDump(DataProvider<Integer, DepthResult> provider) {

        this.lastScanTime = Util.nowMS();
        this.data.forEach((id, symbolDepths) -> dump(id, symbolDepths, provider));
    }


    private void dump(int symbolId, SymbolDepths symbolDepths, DataProvider<Integer, DepthResult> provider) {

        DepthResult depthResult = provider.feedOne(symbolId);
        depthResult.setSymbolId(symbolId);
        depthResult.setTimestamp(Util.nowMS());
        depthResult.dump(symbolDepths, this.maxDepthSize);
        provider.push();
        symbolDepths.completedDump();
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        Validate.isTrue(bytes.readInt() == 0);
        this.data = SerializationUtils.readIntHashMap(bytes, bytesIn -> {

            SymbolDepths symbolDepths = new SymbolDepths();
            symbolDepths.readMarshallable(bytesIn);
            return symbolDepths;
        });
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(0);
        SerializationUtils.writeIntHashMap(this.data, bytes);
    }
}
