package com.github.fevernova.io.hdfs;


import com.alibaba.fastjson.JSONObject;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractBatchSink;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.checkpoint.CheckPointSaver;
import com.github.fevernova.framework.service.checkpoint.ICheckPointSaver;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.io.hdfs.writer.AbstractHDFSWriter;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.List;


@Slf4j
public abstract class AbstractHDFSBatchSink extends AbstractBatchSink implements BarrierCoordinatorListener {


    protected AbstractHDFSWriter hdfsWriter;

    private ICheckPointSaver<HDFSCheckPoint> checkpoints;

    private List<FileInfo> hdfsFilePathList = Lists.newArrayList();


    public AbstractHDFSBatchSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.checkpoints = new CheckPointSaver<>();
    }


    @Override protected void batchWhenBarrierSnaptshot(BarrierData barrierData) {

        this.checkpoints.put(barrierData.getBarrierId(), new HDFSCheckPoint(this.hdfsFilePathList));
        this.hdfsFilePathList = Lists.newLinkedList();

    }


    @Override protected void batchPrepare(Data data) {

        try {
            this.hdfsWriter.open();
        } catch (IOException e) {
            log.error("hdfsWriter prepare error : ", e);
            super.globalContext.fatalError(e.getMessage());
        }
    }


    @Override protected int batchHandleEvent(Data data) {

        try {
            return this.hdfsWriter.writeData(data);
        } catch (IOException e) {
            log.error("hdfsWriter write data error : ", e);
            Validate.isTrue(false);
        }
        return 1;
    }


    @Override protected void batchSync() throws IOException {

        this.hdfsWriter.sync();
    }


    @Override protected void batchClose(boolean bySnapshot) throws Exception {

        if (this.hdfsWriter != null) {
            Pair<String, String> p = this.hdfsWriter.close();
            this.hdfsFilePathList.add(FileInfo.builder().from(p.getKey()).to(p.getValue()).build());
        }
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return this.checkpoints.getCheckPoint(barrierData.getBarrierId()) != null;
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) throws Exception {

        HDFSCheckPoint checkPoint = this.checkpoints.getCheckPoint(barrierData.getBarrierId());
        StateValue stateValue = new StateValue();
        stateValue.setComponentType(super.componentType);
        stateValue.setComponentTotalNum(super.total);
        stateValue.setCompomentIndex(super.index);
        stateValue.setValue(checkPoint);
        return stateValue;
    }


    @Override public void onRecovery(List<StateValue> stateValues) {

        super.onRecovery(stateValues);
        for (StateValue stateValue : stateValues) {
            HDFSCheckPoint checkPoint = new HDFSCheckPoint();
            checkPoint.parseFromJSON((JSONObject) stateValue.getValue());
            if (CollectionUtils.isEmpty(checkPoint.getFiles())) {
                continue;
            }
            for (FileInfo fileInfo : checkPoint.getFiles()) {
                try {
                    this.hdfsWriter.releaseDataFile(fileInfo.getFrom(), fileInfo.getTo());
                } catch (Exception e) {
                    log.error("recovery error ", e);
                    Validate.isTrue(false);
                }
            }
        }
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        if (result) {
            HDFSCheckPoint checkPoint = this.checkpoints.remove(barrierData.getBarrierId());
            Validate.notNull(checkPoint);
            for (FileInfo fileInfo : checkPoint.getFiles()) {
                boolean releaseDataFile = this.hdfsWriter.releaseDataFile(fileInfo.getFrom(), fileInfo.getTo());
                if (releaseDataFile) {
                    log.info(" mv tmp file to target path : " + fileInfo.getTo());
                } else {
                    log.error(" failed to mv file from {} to {} ", fileInfo.getFrom(), fileInfo.getTo());
                    Validate.isTrue(false);
                }
            }
        }
    }
}
