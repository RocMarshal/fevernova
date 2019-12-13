package com.github.fevernova.framework.service.state;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.storage.IStorage;
import com.github.fevernova.framework.service.state.storage.IgnoreStorage;

import java.util.List;


public class StateService {


    private GlobalContext globalContext;

    private TaskContext taskContext;

    private IStorage storage;//TODO 需要实现一个持久化的state存储


    public StateService(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.storage = new IgnoreStorage();
    }


    public void saveStateValues(BarrierData barrierData, List<StateValue> stateValueList) {

        this.storage.save();
    }


    public void achieveStateValues(BarrierData barrierData) {

        this.storage.achieve();
    }


    public void scanStatesForRecovery() {

        this.storage.recovery();
    }

}
