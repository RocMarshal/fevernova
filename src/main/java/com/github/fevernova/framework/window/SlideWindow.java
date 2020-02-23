package com.github.fevernova.framework.window;


import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;


public abstract class SlideWindow<W extends ObjectWithId> implements WriteBytesMarshallable, ReadBytesMarshallable {


    protected IntObjectHashMap<W> windows;

    private long span;

    private int windowNum;

    protected WindowListener<W> windowListener;

    //cache
    private int currentWindowSeq;

    protected W currentWindow;


    public SlideWindow(long span, int windowNum) {

        this(span, windowNum, null);
    }


    public SlideWindow(long span, int windowNum, WindowListener windowListener) {

        this.span = span;
        this.windowNum = windowNum;
        this.windows = new IntObjectHashMap<>(windowNum);
        this.windowListener = windowListener;
    }


    protected boolean prepareCurrentWindow(long timestamp) {

        int windowSeq = (int) (timestamp / this.span);
        if (this.currentWindowSeq == windowSeq) {
            return true;
        }

        this.currentWindowSeq = windowSeq;
        this.currentWindow = this.windows.get(this.currentWindowSeq);
        if (this.currentWindow != null) {
            return true;
        }

        if (this.windows.size() < this.windowNum) {
            this.currentWindow = newWindow(this.currentWindowSeq);
            this.windows.put(this.currentWindowSeq, this.currentWindow);
            if (this.windowListener != null) {
                this.windowListener.createNewWindow(this.currentWindow);
            }
            return true;
        }

        if (this.currentWindowSeq > this.windows.min().getId()) {
            W w = this.windows.min();
            this.windows.remove(w.getId());
            if (this.windowListener != null) {
                this.windowListener.removeOldWindow(w);
            }
            this.currentWindow = newWindow(this.currentWindowSeq);
            this.windows.put(this.currentWindowSeq, this.currentWindow);
            if (this.windowListener != null) {
                this.windowListener.createNewWindow(this.currentWindow);
            }
            return true;
        }
        this.currentWindow = this.windows.max();
        this.currentWindowSeq = this.currentWindow.getId();
        return false;
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
