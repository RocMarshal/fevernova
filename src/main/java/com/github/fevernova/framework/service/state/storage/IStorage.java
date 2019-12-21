package com.github.fevernova.framework.service.state.storage;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.AchieveClean;
import com.github.fevernova.framework.service.state.StateValue;

import java.util.List;


public abstract class IStorage {


    private GlobalContext globalContext;

    private TaskContext taskContext;


    public IStorage(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
    }


    public abstract void saveStateValue(BarrierData barrierData, List<StateValue> stateValueList);


    public abstract void achieveStateValue(BarrierData barrierData, AchieveClean achieveClean);


    public abstract List<StateValue> recoveryStateValue();

}
