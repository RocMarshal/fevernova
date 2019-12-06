package com.github.fevernova.framework.service.checkpoint;


public interface ICheckPointSaver<T extends CheckPoint> {


    void put(long barrierId, T checkPoint);

    T getCheckPoint(long barrierId);

    T remove(long barrierId);
}
