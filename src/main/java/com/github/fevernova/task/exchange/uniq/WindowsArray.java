package com.github.fevernova.task.exchange.uniq;


import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Map;


public class WindowsArray implements WriteBytesMarshallable {


    private final IntObjectHashMap<Window> windows;

    private final Map<Integer, Window> windowsCache;

    private final long span;

    private final int windowNum;

    //cache
    private int currentWindowSeq = 0;

    private Window currentWindow;


    public WindowsArray(long span, int windowNum) {

        this.span = span;
        this.windowNum = windowNum;
        this.windows = new IntObjectHashMap<>(windowNum);
        this.windowsCache = Maps.newHashMapWithExpectedSize(windowNum);
    }


    public WindowsArray(long span, int windowNum, BytesIn bytes) {

        this.span = span;
        this.windowNum = windowNum;
        this.windows = SerializationUtils.readIntMap(bytes, bytesIn -> new Window(bytes));
        this.windowsCache = Maps.newHashMapWithExpectedSize(windowNum);
        this.windows.forEachKeyValue((each, parameter) -> windowsCache.put(each, parameter));
    }


    public boolean unique(long eventId, long timestamp) {

        int windowSeq = (int) (timestamp / this.span);
        if (this.currentWindow == null || this.currentWindowSeq != windowSeq) {
            this.currentWindowSeq = windowSeq;
            this.currentWindow = this.windowsCache.get(windowSeq);
            if (this.currentWindow == null) {
                this.currentWindow = new Window(windowSeq);
                while (this.windows.size() >= this.windowNum) {
                    Window mw = this.windows.min();
                    this.windows.remove(mw.getSeq());
                    this.windowsCache.remove(mw.getSeq());
                }
                this.windows.put(windowSeq, this.currentWindow);
                this.windowsCache.put(windowSeq, this.currentWindow);
            }
        }
        return this.currentWindow.unique(eventId);
    }


    public long count() {

        return this.windows.stream().mapToLong(value -> value.count()).sum();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        SerializationUtils.marshallIntMap(this.windows, bytes);
        this.windows.forEachKeyValue((each, parameter) -> windowsCache.put(each, parameter));
    }
}
