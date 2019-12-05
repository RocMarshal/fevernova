package com.github.fevernova.framework.service.barrier.listener;


import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.StateValue;


public interface BarrierCoordinatorListener {


    boolean collect(BarrierData barrierData) throws Exception;

    StateValue getStateForRecovery(BarrierData barrierData);

    void result(boolean result, BarrierData barrierData) throws Exception;

}
