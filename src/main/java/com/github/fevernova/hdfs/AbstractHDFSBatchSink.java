package com.github.fevernova.hdfs;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractBatchSink;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.hdfs.writer.AbstractHDFSWriter;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public abstract class AbstractHDFSBatchSink extends AbstractBatchSink implements BarrierCoordinatorListener {


    protected AbstractHDFSWriter hdfsWriter;

    private ConcurrentHashMap<Long, List<Pair<String, String>>> toBeMovedFileMap = new ConcurrentHashMap<>();

    private List<Pair<String, String>> hdfsFilePathList;


    public AbstractHDFSBatchSink(GlobalContext globalContext,
                                 TaskContext taskContext,
                                 int index,
                                 int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.hdfsFilePathList = Lists.newLinkedList();
    }


    @Override protected void snapshotWhenBarrierAfterBatch(BarrierData barrierData) {

        this.toBeMovedFileMap.put(barrierData.getBarrierId(), this.hdfsFilePathList);
        this.hdfsFilePathList = Lists.newLinkedList();

    }


    @Override protected void prepare(Data event) {

        try {
            this.hdfsWriter.open();
        } catch (IOException e) {
            super.globalContext.fatalError(e.getMessage());
        }
    }


    @Override protected int handleEventAndReturnSize(Data dataEvent) {

        try {
            return this.hdfsWriter.writeData(dataEvent);
        } catch (IOException e) {
            log.error("hdfsWriter write data error", e);
            Validate.isTrue(false);
        }
        return 1;
    }


    @Override protected void close() throws Exception {

        if (this.hdfsWriter != null) {
            this.hdfsFilePathList.add(this.hdfsWriter.close());
        }
    }


    @Override protected void sendBatch() throws IOException {

        this.hdfsWriter.sync();
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return this.toBeMovedFileMap.containsKey(barrierData.getBarrierId());
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) {

        return null;
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        if (barrierData == null) {
            return;
        }
        long barrierId = barrierData.getBarrierId();
        if (this.toBeMovedFileMap.containsKey(barrierId)) {
            List<Pair<String, String>> filePathList = this.toBeMovedFileMap.get(barrierId);
            if (!filePathList.isEmpty()) {
                for (Pair<String, String> path : filePathList) {
                    boolean releaseDataFile = this.hdfsWriter.releaseDataFile(path.getLeft(), path.getRight());
                    if (releaseDataFile) {
                        log.info(" mv tmp file to target path :" + path.getRight());
                    } else {
                        log.error(" failed to mv file from {} to {} ", path.getLeft(), path.getRight());
                        Validate.isTrue(false);
                    }
                }
            }
            this.toBeMovedFileMap.remove(barrierId);
        }
    }
}
