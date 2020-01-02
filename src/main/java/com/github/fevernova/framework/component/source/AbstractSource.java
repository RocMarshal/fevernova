package com.github.fevernova.framework.component.source;


import com.github.fevernova.framework.common.Util;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.component.Component;
import com.github.fevernova.framework.component.ComponentStatus;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import com.github.fevernova.framework.component.channel.WaitNotify;
import com.github.fevernova.framework.service.barrier.listener.BarrierEmitListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public abstract class AbstractSource<K, V extends Data> extends Component implements Runnable, BarrierEmitListener, WaitNotify, DataProvider<K, V> {


    private final ArrayBlockingQueue<BarrierData> barriersQueue = new ArrayBlockingQueue<>(10);

    protected ChannelProxy<K, V> channelProxy;

    private AtomicInteger barriersNum = new AtomicInteger(0);


    public AbstractSource(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum);
        this.channelProxy = channelProxy;
    }


    @Override public void run() {

        try {
            onStart();
            while (super.status.inLoop) {
                while (this.barriersNum.get() > 0) {
                    BarrierData barrierData = this.barriersQueue.poll();
                    Validate.notNull(barrierData);
                    this.barriersNum.decrementAndGet();
                    onBarrierData(barrierData);
                }
                if (this.status == ComponentStatus.RUNNING) {
                    work();
                } else if (this.status == ComponentStatus.PAUSE) {
                    Util.sleepMS(1);
                }
            }
        } catch (Throwable e) {
            super.globalContext.fatalError("Source Fatal Error : ", e);
        }
    }


    public abstract void work();


    @Override public V feedOne(K key) {

        return this.channelProxy.feed(key);
    }


    @Override public void push() {

        this.channelProxy.push();
    }


    @Override public void emit(BarrierData barrierData) {

        try {
            this.barriersQueue.put(barrierData);
            this.barriersNum.incrementAndGet();
        } catch (InterruptedException e) {
            super.globalContext.fatalError("Source cache barrier error : ", e);
        }
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

    }


    @Override protected void distributeBarrier(BarrierData barrierData) {

        this.channelProxy.distributeBarrier(barrierData);
    }


    public void onBroadcastData(BroadcastData broadcastData) {

        this.channelProxy.broadcast(onBroadcast(broadcastData));
    }


    @Override protected BroadcastData onBroadcast(BroadcastData broadcastData) {

        return broadcastData;
    }


    @Override public void waitTime(long nanos) {

        this.waitTime.inc(nanos);
    }
}
