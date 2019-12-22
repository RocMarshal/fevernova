package com.github.fevernova.framework.service.state;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.service.state.storage.IStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.ReadBytesMarshallable;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.Validate;

import java.util.List;


@Slf4j
public class StateService {


    private GlobalContext globalContext;

    private TaskContext taskContext;

    private IStorage storage;

    @Getter
    private boolean supportRecovery;

    private AchieveClean achieveClean;


    public StateService(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.supportRecovery = taskContext.getBoolean("recovery", false);
        String type = taskContext.getString("storagetype", "Ignore");
        try {
            this.storage = (IStorage) Class.forName("com.github.fevernova.framework.service.state.storage." + type + "Storage")
                    .getConstructor(GlobalContext.class, TaskContext.class).newInstance(globalContext, taskContext);
        } catch (Exception e) {
            log.error("StateService error : ", e);
            Validate.isTrue(false);
        }
        this.achieveClean = AchieveClean.valueOf(taskContext.getString("achieveclean", "all").toUpperCase());
    }


    public void saveStateValues(BarrierData barrierData, List<StateValue> stateValueList) {

        this.storage.saveStateValue(barrierData, stateValueList);
    }


    public void achieveStateValues(BarrierData barrierData) {

        this.storage.achieveStateValue(barrierData, this.achieveClean);
    }


    public List<StateValue> recoveryStateValue() {

        return this.storage.recoveryStateValue();
    }


    public String saveBinary(BinaryFileIdentity identity, BarrierData barrierData, WriteBytesMarshallable obj) {

        return this.storage.saveBinary(identity, barrierData, obj);
    }


    public void recoveryBinary(String stateFilePath, ReadBytesMarshallable obj) {

        this.storage.recoveryBinary(stateFilePath, obj);
    }
}
