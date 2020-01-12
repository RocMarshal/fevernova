package com.github.fevernova.task.exchange.uniq;


import com.github.fevernova.framework.common.context.ContextObject;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.io.hdfs.Constants;
import com.github.fevernova.task.exchange.SerializationUtils;
import com.google.common.collect.Maps;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.core.io.IORuntimeException;
import org.apache.commons.lang3.Validate;

import java.util.Map;


public class SlideWindowFilter extends ContextObject implements WriteBytesMarshallable, ReadBytesMarshallable {


    public static final String CONS_NAME = "SlideWindowFilter";

    private final long span;

    private final int windowNum;

    private Map<Integer, WindowsArray> filter;

    //cache
    private int currentTypeId = 0;

    private WindowsArray currentWindows;


    public SlideWindowFilter(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
        this.span = taskContext.getLong("span", 60 * 1000L);
        Validate.isTrue(this.span % 60000 == 0 && Constants.MINUTE_PERIOD_SET.contains((int) (this.span / 60000)));
        this.windowNum = taskContext.getInteger("num", 10);
        this.filter = Maps.newHashMap();
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

        return this.filter.entrySet().stream().mapToLong(value -> value.getValue().count()).sum();
    }


    @Override public void writeMarshallable(final BytesOut bytes) {

        SerializationUtils.writeIntHashMap(this.filter, bytes);
    }


    @Override public void readMarshallable(BytesIn bytes) throws IORuntimeException {

        this.filter = SerializationUtils.readIntHashMap(bytes, bytesIn -> new WindowsArray(span, windowNum, bytesIn));
    }
}
