package com.github.fevernova.framework.service.barrier.listener;


import com.github.fevernova.framework.common.data.BarrierData;


public interface BarrierCoordinatorListener {


    boolean collect(BarrierData barrierData) throws Exception;

    void result(boolean result, BarrierData barrierData) throws Exception;

}
