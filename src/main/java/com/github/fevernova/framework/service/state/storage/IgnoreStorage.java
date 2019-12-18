package com.github.fevernova.framework.service.state.storage;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.AchieveClean;
import com.github.fevernova.framework.service.state.StateValue;

import java.util.List;


public class IgnoreStorage extends IStorage {


    public IgnoreStorage(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
    }


    @Override public void save(BarrierData barrierData, List<StateValue> stateValueList) {

    }


    @Override public void achieve(BarrierData barrierData, AchieveClean achieveClean) {

    }


    @Override public List<StateValue> recovery() {

        return null;
    }


}
