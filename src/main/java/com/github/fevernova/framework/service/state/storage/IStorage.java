package com.github.fevernova.framework.service.state.storage;


import com.github.fevernova.framework.common.context.ContextObject;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.AchieveClean;
import com.github.fevernova.framework.service.state.BinaryFileIdentity;
import com.github.fevernova.framework.service.state.StateValue;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.List;


public abstract class IStorage extends ContextObject {


    public IStorage(GlobalContext globalContext, TaskContext taskContext) {

        super(globalContext, taskContext);
    }


    public abstract void saveStateValue(BarrierData barrierData, List<StateValue> stateValueList);


    public abstract void achieveStateValue(BarrierData barrierData, AchieveClean achieveClean);


    public abstract List<StateValue> recoveryStateValue();


    public abstract String saveBinary(BinaryFileIdentity identity, BarrierData barrierData, WriteBytesMarshallable obj);


    public abstract void recoveryBinary(String stateFilePath, ReadBytesMarshallable obj);

}
