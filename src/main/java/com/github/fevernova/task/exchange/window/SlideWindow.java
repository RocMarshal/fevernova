package com.github.fevernova.task.exchange.window;


import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;


public abstract class SlideWindow<W extends ObjectWithId> implements WriteBytesMarshallable, ReadBytesMarshallable {


    private IntObjectHashMap<W> windows;

    private long span;

    private int windowNum;

    //cache
    private int currentWindowSeq;

    protected W currentWindow;


    public SlideWindow(long span, int windowNum) {

        this.span = span;
        this.windowNum = windowNum;
        this.windows = new IntObjectHashMap<>(windowNum);
    }


    protected void prepareCurrentWindow(long timestamp) {

        int windowSeq = (int) (timestamp / this.span);
        if (this.currentWindowSeq != windowSeq) {
            this.currentWindowSeq = windowSeq;
            this.currentWindow = this.windows.get(this.currentWindowSeq);
            if (this.currentWindow == null) {
                this.currentWindow = newWindow(this.currentWindowSeq);
                this.windows.put(this.currentWindowSeq, this.currentWindow);
                if (this.windows.size() >= this.windowNum) {
                    W w = this.windows.min();
                    this.windows.remove(w.getId());
                    this.currentWindow = this.windows.min();
                    this.currentWindowSeq = this.currentWindow.getId();
                }
            }
        }
    }


    protected abstract W newWindow(int seq);


    @Override public void writeMarshallable(BytesOut bytes) {

        bytes.writeInt(this.windows.size());
        this.windows.forEachKeyValue((k, v) -> {
            bytes.writeInt(k);
            v.writeMarshallable(bytes);
        });
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        int length = bytes.readInt();
        for (int i = 0; i < length; i++) {
            int k = bytes.readInt();
            W w = newWindow(k);
            w.readMarshallable(bytes);
            this.windows.put(k, w);
        }
    }

}
