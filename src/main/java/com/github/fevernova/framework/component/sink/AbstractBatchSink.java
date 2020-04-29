package com.github.fevernova.framework.component.sink;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.io.IOException;


@Slf4j
public abstract class AbstractBatchSink extends AbstractSink {


    protected boolean inBatch = false;

    protected long syncSize;

    protected long rollingSize;

    protected long rollingPeriod;

    protected long accumulateSize4Sync;

    protected long accumulateSize4Rolling;

    protected long lastRollingSeq;


    public AbstractBatchSink(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum) {

        super(globalContext, taskContext, index, inputsNum);
        this.syncSize = super.taskContext.getLong("syncsize", 8 * 1024 * 1024L);
        this.rollingSize = super.taskContext.getLong("rollingsize", 256 * 1024 * 1024L);
        this.rollingPeriod = super.taskContext.getLong("flushperiod", 5 * 60 * 1000L);
        this.lastRollingSeq = Util.nowMS() / this.rollingPeriod;
    }


    @Override protected void handleEvent(Data event) {

        if (!this.inBatch) {
            batchPrepare(event);
            this.inBatch = true;
        }
        int dataSize = batchHandleEvent(event);
        this.accumulateSize4Sync += dataSize;
        this.accumulateSize4Rolling += dataSize;

        if (isSizeReady4Rolling()) {
            syncBatch();
            closeBatch(false);
        } else if (isSizeReady4Sync()) {
            syncBatch();
        }
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

        if (isTimeReady4Rolling(barrierData.getTimestamp())) {
            if (this.inBatch) {
                syncBatch();
                closeBatch(true);
            }
            this.lastRollingSeq = barrierData.getTimestamp() / this.rollingPeriod;
            batchWhenBarrierSnaptshot(barrierData);
        }
    }


    private boolean isSizeReady4Sync() {

        return this.accumulateSize4Sync >= this.syncSize;
    }


    private boolean isTimeReady4Rolling(long barrierTimestamp) {

        return (barrierTimestamp / this.rollingPeriod) > this.lastRollingSeq;
    }


    private boolean isSizeReady4Rolling() {

        return this.accumulateSize4Rolling >= this.rollingSize;
    }


    protected void syncBatch() {

        try {
            batchSync();
            this.accumulateSize4Sync = 0L;
        } catch (Throwable e) {
            log.error("sync : ", e);
            Validate.isTrue(false);
        }
    }


    protected void closeBatch(boolean bySnapshot) {

        try {
            batchClose(bySnapshot);
            this.inBatch = false;
            this.accumulateSize4Rolling = 0L;
        } catch (Throwable e) {
            log.error("close : ", e);
            Validate.isTrue(false);
        }
    }


    protected abstract void batchPrepare(Data event);

    protected abstract int batchHandleEvent(Data dataEvent);

    protected abstract void batchSync() throws IOException;

    protected abstract void batchClose(boolean bySnapshot) throws Exception;

    protected abstract void batchWhenBarrierSnaptshot(BarrierData barrierData);

}
