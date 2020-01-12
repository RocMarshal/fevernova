package com.github.fevernova.task.exchange.uniq;


import com.github.fevernova.task.exchange.SerializationUtils;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;


public class WindowsArray implements WriteBytesMarshallable {


    private final IntObjectHashMap<Window> windows;

    private final long span;

    private final int windowNum;

    //cache
    private int currentWindowSeq = 0;

    private Window currentWindow;


    public WindowsArray(long span, int windowNum) {

        this.span = span;
        this.windowNum = windowNum;
        this.windows = new IntObjectHashMap<>(windowNum);
    }


    public WindowsArray(long span, int windowNum, BytesIn bytes) {

        this.span = span;
        this.windowNum = windowNum;
        this.windows = SerializationUtils.readIntMap(bytes, bytesIn -> new Window(bytes));
    }


    public boolean unique(long eventId, long timestamp) {

        int windowSeq = (int) (timestamp / this.span);
        if (this.currentWindow == null || this.currentWindowSeq != windowSeq) {
            this.currentWindowSeq = windowSeq;
            this.currentWindow = this.windows.get(windowSeq);
            if (this.currentWindow == null) {
                this.currentWindow = new Window(windowSeq);
                while (this.windows.size() >= this.windowNum) {
                    Window mw = this.windows.min();
                    this.windows.remove(mw.getSeq());
                }
                this.windows.put(windowSeq, this.currentWindow);
            }
        }
        return this.currentWindow.unique(eventId);
    }


    public long count() {

        return this.windows.stream().mapToLong(value -> value.count()).sum();
    }


    @Override public void writeMarshallable(BytesOut bytes) {

        SerializationUtils.writeIntMap(this.windows, bytes);
    }
}
