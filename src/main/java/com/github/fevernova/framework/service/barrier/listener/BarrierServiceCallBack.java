package com.github.fevernova.framework.service.barrier.listener;


import com.github.fevernova.framework.common.data.BarrierData;


public interface BarrierServiceCallBack {


    void ackBarrier(String key, BarrierData barrierData);

}
