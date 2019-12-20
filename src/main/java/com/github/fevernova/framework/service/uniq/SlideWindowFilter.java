package com.github.fevernova.framework.service.uniq;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.hdfs.Constants;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.lang3.Validate;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;


public class SlideWindowFilter implements WriteBytesMarshallable, ReadBytesMarshallable {


    private final GlobalContext globalContext;

    private final TaskContext taskContext;

    private final long span;

    private final int windowNum;

    @Getter
    private LongObjectHashMap<LongObjectHashMap<Window>> filter;


    public SlideWindowFilter(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.span = taskContext.getLong("span", 60 * 1000L);
        Validate.isTrue(this.span % 60000 == 0 && Constants.MINUTE_PERIOD_SET.contains((int) (this.span / 60000)));
        this.windowNum = taskContext.getInteger("num", 10);
        this.filter = new LongObjectHashMap();
    }


    public boolean uniq(long typeId, long eventId, long timestamp) {

        LongObjectHashMap<Window> windows = this.filter.get(typeId);
        if (windows == null) {
            windows = new LongObjectHashMap<>(this.windowNum);
            this.filter.put(typeId, windows);
        }
        long windowSeq = timestamp / this.span;
        Window window = windows.get(windowSeq);
        if (window == null) {
            window = new Window(windowSeq);
            while (windows.size() >= this.windowNum) {
                Window mw = windows.min();
                windows.remove(mw.getSeq());
            }
            windows.put(windowSeq, window);
        }
        return window.uniq(eventId);
    }


    @Override public void writeMarshallable(final BytesOut bytes) {

        SerializationUtils.marshallLongLongMap(this.filter, bytes);
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.filter = SerializationUtils.readLongLongMap(bytes, bytesIn -> new Window(bytesIn));
    }
}
