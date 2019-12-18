package com.github.fevernova.framework.component;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.Named;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.metric.MetricName;
import com.github.fevernova.framework.metric.UnitCons;
import com.github.fevernova.framework.metric.UnitCounter;
import com.github.fevernova.framework.service.barrier.listener.BarrierServiceCallBack;
import com.github.fevernova.framework.task.Manager;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public abstract class Component extends LifecycleAwareExtension {


    protected final GlobalContext globalContext;

    protected final TaskContext taskContext;

    protected final Named named;

    @Getter
    protected final ComponentType componentType;

    @Getter
    protected final int index;

    protected final int total;

    protected final int inputsNum;

    protected final UnitCounter handleRows;

    protected final UnitCounter waitTime;

    protected final Map<Long, AtomicInteger> barriersCounter;

    protected volatile ComponentStatus status;

    @Setter
    protected BarrierServiceCallBack barrierServiceCallBack;


    public Component(GlobalContext globalContext,
                     TaskContext taskContext,
                     int index,
                     int inputsNum) {

        this.globalContext = globalContext;
        this.taskContext = taskContext;
        this.index = index;
        this.total = taskContext.getInteger(Constants.PARALLELISM);
        this.inputsNum = inputsNum;
        this.named = Named.builder().taskName(globalContext.getJobTags().getJobType()).moduleName(taskContext.getName())
                .moduleType(this.getClass().getSimpleName()).index(index).total(this.total).build();
        this.componentType = ComponentType.valueOf(taskContext.getName().toUpperCase());
        this.status = ComponentStatus.INIT;
        this.barriersCounter = Maps.newHashMap();
        this.handleRows = new UnitCounter(this.componentType, MetricName.HANDLE_ROWS, UnitCons.ROW);
        this.waitTime = new UnitCounter(this.componentType, MetricName.WAIT_TIME, UnitCons.MS, 1000000L);
    }


    @Override public void init() {

        Manager.getInstance().getMonitorService().register(this.handleRows.getRegisterName(this.named), this.handleRows);
        Manager.getInstance().getMonitorService().register(this.waitTime.getRegisterName(this.named), this.waitTime);
    }


    @Override
    public void onStart() {

        this.status = ComponentStatus.RUNNING;
        log.info(this.named.render(true) + " starting at " + LocalDateTime.now());
    }


    @Override public void onRecovery() {

        log.info(this.named.render(true) + " recovery at " + LocalDateTime.now());
    }


    public void onPause() {

        this.status = ComponentStatus.PAUSE;
        log.info(this.named.render(true) + " waiting at " + LocalDateTime.now());
    }


    public void onResume() {

        this.status = ComponentStatus.RUNNING;
        log.info(this.named.render(true) + " resume to running at " + LocalDateTime.now());
    }


    @Override
    public void onShutdown() {

        this.status = ComponentStatus.CLOSING;
        log.info(this.named.render(true) + " ending at " + LocalDateTime.now());
    }


    public void onBarrierData(BarrierData barrierData) {

        if (this.inputsNum <= 1) {
            this.onBarrier(barrierData);
            return;
        }
        AtomicInteger counter = this.barriersCounter.get(barrierData.getBarrierId());
        if (counter == null) {
            counter = new AtomicInteger(0);
            this.barriersCounter.put(barrierData.getBarrierId(), counter);
        }
        if (this.inputsNum == counter.incrementAndGet()) {
            this.onBarrier(barrierData);
            this.barriersCounter.remove(barrierData.getBarrierId());
        }

    }


    protected void onBarrier(BarrierData barrierData) {

        snapshotWhenBarrier(barrierData);
        this.barrierServiceCallBack.ackBarrier(this.named.render(true), barrierData);
        distributeBarrier(barrierData);
    }


    protected boolean isFirst() {

        return this.index == 0;
    }


    protected abstract void snapshotWhenBarrier(BarrierData barrierData);

    protected abstract void distributeBarrier(BarrierData barrierData);

    protected abstract void onBroadcastData(BroadcastData broadcastData);

    protected abstract BroadcastData onBroadcast(BroadcastData broadcastData);

}
