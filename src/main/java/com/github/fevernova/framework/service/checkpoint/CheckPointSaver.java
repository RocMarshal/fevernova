package com.github.fevernova.framework.service.checkpoint;


import com.google.common.collect.Maps;

import java.util.Map;


public class CheckPointSaver<T extends CheckPoint> implements ICheckPointSaver<T> {


    protected Map<Long, T> checkPoints = Maps.newConcurrentMap();


    @Override
    public void put(long barrierId, T checkPoint) {

        this.checkPoints.put(barrierId, checkPoint);
    }


    @Override
    public T getCheckPoint(long barrierId) {

        return this.checkPoints.remove(barrierId);
    }
}
