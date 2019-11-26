package com.github.fevernova.framework.service.barrier.listener;


import com.github.fevernova.framework.common.data.BarrierData;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public interface BarrierCoordinatorListener {


    BarrierData collect(BarrierData barrierData) throws Exception;

    default boolean isMaster() {

        return false;
    }

    default BarrierData select(List<BarrierData> barrierDataList) {

        if (barrierDataList == null) {
            return null;
        }
        barrierDataList.removeAll(Collections.singleton(null));
        if (barrierDataList.size() > 0) {
            return barrierDataList.stream().min(Comparator.comparingLong(BarrierData::getBarrierId)).get();
        }
        return null;
    }

    void submit(BarrierData barrierData) throws Exception;

}
