package com.github.fevernova.framework.service.barrier;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.component.Component;
import com.github.fevernova.framework.service.barrier.listener.BarrierCompletedListener;
import com.github.fevernova.framework.service.barrier.listener.BarrierCoordinatorListener;
import com.github.fevernova.framework.service.barrier.listener.BarrierEmitListener;
import com.github.fevernova.framework.service.barrier.listener.BarrierServiceCallBack;
import com.github.fevernova.framework.task.TaskTopology;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class BarrierService implements BarrierServiceCallBack {


    private final AtomicLong barrierSequence = new AtomicLong(0);

    private final ConcurrentSkipListMap<Long, Pair<BarrierData, AtomicInteger>> unCompletedBarriers = new ConcurrentSkipListMap();

    private final List<BarrierEmitListener> barrierEmitListeners = Lists.newArrayList();

    private final List<BarrierCoordinatorListener> barrierCoordinatorListeners = Lists.newArrayList();

    private final List<BarrierCompletedListener> barrierCompletedListeners = Lists.newArrayList();

    private GlobalContext globalContext;

    private int componentsNum = 0;

    private ThreadPoolExecutor executors = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(16));


    public BarrierService(GlobalContext globalContext, TaskContext taskContext) {

        this.globalContext = globalContext;
    }


    public void register(TaskTopology taskTopology) {

        taskTopology.getComponents().forEach(component -> register(component));
    }


    private void register(Component component) {

        this.componentsNum++;
        component.setBarrierServiceCallBack(this);
        if (component instanceof BarrierEmitListener) {
            this.barrierEmitListeners.add((BarrierEmitListener) component);
        }
        if (component instanceof BarrierCompletedListener) {
            this.barrierCompletedListeners.add((BarrierCompletedListener) component);
        }
        if (component instanceof BarrierCoordinatorListener) {
            BarrierCoordinatorListener coordinatorCPListener = (BarrierCoordinatorListener) component;
            this.barrierCoordinatorListeners.add(coordinatorCPListener);
        }
    }


    public BarrierData generateBarrier() {

        BarrierData barrierData = new BarrierData(this.barrierSequence.incrementAndGet(), Util.nowMS());
        this.unCompletedBarriers.put(barrierData.getBarrierId(), Pair.of(barrierData, new AtomicInteger(this.componentsNum)));
        this.barrierEmitListeners.forEach(barrierStartingListener -> barrierStartingListener.emit(barrierData));
        return barrierData;
    }


    public boolean isBarrierRunning(BarrierData barrierData) {

        return this.unCompletedBarriers.containsKey(barrierData.getBarrierId());
    }


    @Override
    public void ackBarrier(String key, BarrierData barrierData) {

        log.debug("AckBarrier " + barrierData.getBarrierId() + " by " + key);
        Pair<BarrierData, AtomicInteger> pair = this.unCompletedBarriers.get(barrierData.getBarrierId());
        if (pair == null) {
            this.globalContext.fatalError("Barrier Missing1 :" + barrierData.toString());
        }
        int s = pair.getRight().decrementAndGet();
        if (s > 0) {
            return;
        } else if (s == 0) {
            if (this.unCompletedBarriers.firstKey() == barrierData.getBarrierId()) {
                this.unCompletedBarriers.remove(barrierData.getBarrierId());
                notifyListeners(barrierData);
            } else {
                this.globalContext.fatalError("Barrier Missing2 :" + barrierData.toString());
            }
        } else {
            this.globalContext.fatalError("Barrier Missing3 :" + barrierData.toString());
        }
    }


    protected void notifyListeners(final BarrierData barrierData) {

        Thread thread = new Thread(() -> {
            boolean coordinatorResult = true;
            if (!barrierCoordinatorListeners.isEmpty()) {
                try {
                    List<Boolean> coordinatorCollectResult = Lists.newArrayList();
                    for (BarrierCoordinatorListener barrierCoordinatorListener : barrierCoordinatorListeners) {
                        coordinatorCollectResult.add(barrierCoordinatorListener.collect(barrierData));
                    }
                    coordinatorResult = coordinatorCollectResult.stream().anyMatch(result -> !result);
                    for (BarrierCoordinatorListener barrierCoordinatorListener : barrierCoordinatorListeners) {
                        barrierCoordinatorListener.result(coordinatorResult, barrierData);
                    }
                } catch (Throwable e) {
                    this.globalContext.fatalError("BarrierService.coordinator Error", e);
                }
            }

            for (BarrierCompletedListener barrierCompletedListener : barrierCompletedListeners) {
                try {
                    barrierCompletedListener.completed(barrierData, coordinatorResult);
                } catch (Throwable e) {
                    log.error("barrierCompletedListener completed error", e);
                }
            }
        });
        try {
            this.executors.submit(thread);
        } catch (Throwable e) {
            this.globalContext.fatalError("BarrierService.notifyListeners", e);
        }
    }


}
