package com.github.fevernova.hdfs;


import com.alibaba.fastjson.JSON;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.component.sink.AbstractBatchSink;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.state.StateValue;
import com.github.fevernova.framework.task.Manager;
import com.github.fevernova.hdfs.writer.AbstractHDFSWriter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


@Slf4j
public abstract class AbstractHDFSBatchSink extends AbstractBatchSink implements BarrierCoordinatorListener {


    protected AbstractHDFSWriter hdfsWriter;

    private ConcurrentHashMap<Long, List<KV>> toBeMovedFileMap = new ConcurrentHashMap<>();

    private List<KV> hdfsFilePathList = Lists.newLinkedList();


    public AbstractHDFSBatchSink(GlobalContext globalContext,
                                 TaskContext taskContext,
                                 int index,
                                 int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
    }


    @Override protected void snapshotWhenBarrierAfterBatch(BarrierData barrierData) {

        this.toBeMovedFileMap.put(barrierData.getBarrierId(), this.hdfsFilePathList);
        this.hdfsFilePathList = Lists.newLinkedList();

    }


    @Override protected void prepare(Data data) {

        try {
            this.hdfsWriter.open();
        } catch (IOException e) {
            log.error("hdfsWriter prepare error : ", e);
            super.globalContext.fatalError(e.getMessage());
        }
    }


    @Override protected int handleEventAndReturnSize(Data data) {

        try {
            return this.hdfsWriter.writeData(data);
        } catch (IOException e) {
            log.error("hdfsWriter write data error : ", e);
            Validate.isTrue(false);
        }
        return 1;
    }


    @Override protected void close() throws Exception {

        if (this.hdfsWriter != null) {
            Pair<String, String> p = this.hdfsWriter.close();
            KV kv = new KV();
            kv.k = p.getKey();
            kv.v = p.getValue();
            this.hdfsFilePathList.add(kv);
        }
    }


    @Override protected void sendBatch() throws IOException {

        this.hdfsWriter.sync();
    }


    @Override public boolean collect(BarrierData barrierData) throws Exception {

        return this.toBeMovedFileMap.containsKey(barrierData.getBarrierId());
    }


    @Override public StateValue getStateForRecovery(BarrierData barrierData) {

        List<KV> filePathList = this.toBeMovedFileMap.get(barrierData.getBarrierId());
        StateValue stateValue = new StateValue();
        stateValue.setComponentType(super.componentType);
        stateValue.setComponentTotalNum(super.total);
        stateValue.setCompomentIndex(super.index);
        stateValue.setValue(Maps.newHashMap());
        stateValue.getValue().put("files", JSON.toJSONString(filePathList));
        return stateValue;
    }


    @Override public void onRecovery() {

        super.onRecovery();
        if (super.index == 0) {
            List<StateValue> history = Manager.getInstance().getStateService().recovery();
            if (CollectionUtils.isEmpty(history)) {
                return;
            }
            for (StateValue stateValue : history) {
                if (stateValue.getComponentType() == super.componentType) {
                    List<KV> filePathList = JSON.parseArray(stateValue.getValue().get("files"), KV.class);
                    if (CollectionUtils.isEmpty(filePathList)) {
                        continue;
                    }
                    for (KV kv : filePathList) {
                        try {
                            this.hdfsWriter.releaseDataFile(kv.k, kv.v);
                        } catch (Exception e) {
                            log.error("recovery error ", e);
                            Validate.isTrue(false);
                        }
                    }
                }
            }
        }
    }


    @Override public void result(boolean result, BarrierData barrierData) throws Exception {

        if (result) {
            List<KV> filePathList = this.toBeMovedFileMap.remove(barrierData.getBarrierId());
            Validate.notNull(filePathList);
            if (!filePathList.isEmpty()) {
                for (KV path : filePathList) {
                    boolean releaseDataFile = this.hdfsWriter.releaseDataFile(path.k, path.v);
                    if (releaseDataFile) {
                        log.info(" mv tmp file to target path : " + path.v);
                    } else {
                        log.error(" failed to mv file from {} to {} ", path.k, path.v);
                        Validate.isTrue(false);
                    }
                }
            }
        }
    }


    class KV {


        public String k;

        public String v;
    }
}
