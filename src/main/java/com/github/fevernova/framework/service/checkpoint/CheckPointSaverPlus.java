package com.github.fevernova.framework.service.checkpoint;


import java.util.Iterator;
import java.util.Map;


public class CheckPointSaverPlus<T extends CheckPoint> extends CheckPointSaver<T> {


    @Override public T remove(long barrierId) {

        T checkpoint = super.checkPoints.get(barrierId);
        for (Iterator<Map.Entry<Long, T>> iterator = super.checkPoints.entrySet().iterator(); iterator.hasNext(); ) {
            if (iterator.next().getKey() <= barrierId) {
                iterator.remove();
            }
        }
        return checkpoint;
    }
}
