package com.github.fevernova.framework.service.barrier.listener;


import com.github.fevernova.framework.common.data.BarrierData;


public interface BarrierCompletedListener {


    //抛异常,任务会继续运行
    void completed(BarrierData barrierData, boolean coordinatorResult) throws Exception;

}
