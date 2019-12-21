package com.github.fevernova.framework.service.uniq;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.hdfs.Constants;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.lang3.Validate;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;


public class SlideWindowFilter implements WriteBytesMarshallable, ReadBytesMarshallable {


    private final GlobalContext globalContext;

    private final TaskContext taskContext;

    private final long span;

    private final int windowNum;

    private IntObjectHashMap<WindowsArray> filter;

    //cache
    private int currentTypeId = 0;

    private WindowsArray currentWindows;


    public SlideWindowFilter(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.span = taskContext.getLong("span", 60 * 1000L);
        Validate.isTrue(this.span % 60000 == 0 && Constants.MINUTE_PERIOD_SET.contains((int) (this.span / 60000)));
        this.windowNum = taskContext.getInteger("num", 10);
        this.filter = new IntObjectHashMap();
    }


    public boolean unique(int typeId, long eventId, long timestamp) {

        if (this.currentWindows == null || this.currentTypeId != typeId) {
            this.currentTypeId = typeId;
            this.currentWindows = this.filter.get(typeId);
            if (this.currentWindows == null) {
                this.currentWindows = new WindowsArray(this.span, this.windowNum);
                this.filter.put(typeId, this.currentWindows);
            }
        }
        return this.currentWindows.unique(eventId, timestamp);
    }


    public long count() {

        return this.filter.stream().mapToLong(value -> value.count()).sum();
    }


    @Override public void writeMarshallable(final BytesOut bytes) {

        SerializationUtils.marshallIntMap(this.filter, bytes);
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.filter = SerializationUtils.readIntMap(bytes, bytesIn -> new WindowsArray(span, windowNum, bytesIn));
    }
}