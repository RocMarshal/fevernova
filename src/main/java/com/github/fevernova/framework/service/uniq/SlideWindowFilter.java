package com.github.fevernova.framework.service.uniq;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


public class SlideWindowFilter {


    private GlobalContext globalContext;

    private TaskContext taskContext;

    private long span;

    private long windowNum;

    private IntObjectHashMap<LongObjectHashMap<Roaring64NavigableMap>> filter = IntObjectHashMap.newMap();


    public SlideWindowFilter(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.span = taskContext.getLong("span", 60 * 1000L);
        this.windowNum = taskContext.getLong("num", 10L);
    }


    public boolean uniq(int typeId, long eventId, long timestamp) {

        return true;
    }
}
