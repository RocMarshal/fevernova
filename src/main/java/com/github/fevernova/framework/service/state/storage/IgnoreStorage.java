package com.github.fevernova.framework.service.state.storage;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.AchieveClean;
import com.github.fevernova.framework.service.state.BinaryFileIdentity;
import com.github.fevernova.framework.service.state.StateValue;
import com.google.common.collect.Lists;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;


public class IgnoreStorage extends IStorage {


    public IgnoreStorage(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
    }


    @Override public void saveStateValue(BarrierData barrierData, List<StateValue> stateValueList) {

    }


    @Override public void achieveStateValue(BarrierData barrierData, AchieveClean achieveClean) {

    }


    @Override public List<StateValue> recoveryStateValue() {

        return Lists.newArrayList();
    }


    @Override public String saveBinary(BinaryFileIdentity identity, BarrierData barrierData, WriteBytesMarshallable obj) {

        return null;
    }


    @Override public void recoveryBinary(String stateFilePath, ReadBytesMarshallable obj) {

    }

}
