package com.github.fevernova.framework.component.channel;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.Named;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.*;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.component.channel.selector.ISelector;
import com.github.fevernova.framework.service.aligned.Aligner;
import com.github.fevernova.framework.task.Manager;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class ChannelProxy<K, V extends Data> {


    private static long BALANCE_LEVEL = 8;

    protected Named named;

    private GlobalContext globalContext;

    private List<RingbufferChannel> channelList = Lists.newArrayList();

    private AtomicInteger availbleNum;

    private RingbufferChannel tmpChannel;

    private long sequence;

    private ISelector<K> selector;

    private DataEventFactory dataEventFactory;

    private boolean dynamicBalance;

    private Aligner aligner;

    private int index;

    private boolean isExactlyOnce;


    public ChannelProxy(GlobalContext globalContext,
                        TaskContext taskContext,
                        List<RingbufferChannel> channels,
                        AtomicInteger availbleNum,
                        DataEventFactory dataEventFactory,
                        ISelector selector,
                        int index,
                        boolean dynamicBalance) {

        this.globalContext = globalContext;
        this.channelList.addAll(channels);
        this.availbleNum = availbleNum;
        this.dataEventFactory = dataEventFactory;
        this.selector = selector;
        this.index = index;
        this.named =
                Named.builder().taskName(globalContext.getJobTags().getJobType()).moduleName(taskContext.getName())
                        .moduleType(this.getClass().getSimpleName()).index(this.index).total(taskContext.getInteger(Constants.PARALLELISM)).build();

        this.dynamicBalance = dynamicBalance;
        this.aligner = Manager.getInstance().getAlignService().getAligner(taskContext.getName(), taskContext.getInteger(Constants.PARALLELISM));
        this.isExactlyOnce = Manager.getInstance().getBarrierService().isExactlyOnce();
        if (this.isExactlyOnce) {
            Validate.isTrue(!this.dynamicBalance, "dynamicBalance can not be true, when excactly once is true");
        }
    }


    public void init() {

    }


    public void distributeBarrier(BarrierData barrierData) {

        this.channelList.forEach(channel -> {
            long seq = channel.getNextSeq();
            DataEvent dataEvent = channel.getBySeq(seq);
            dataEvent.setDataType(DataType.BARRIER);
            dataEvent.setData(barrierData);
            channel.pushBySeq(seq);
        });
        if (this.isExactlyOnce) {
            try {
                this.aligner.align();
            } catch (Exception e) {
                this.globalContext.fatalError("alignment waiting error ,", e);
            }
        }
    }


    public void broadcast(BroadcastData broadcastData) {

        if (broadcastData.onlyOnce) {
            if (broadcastData.align) {
                this.aligner.align();
            }
            //GlobleOnce 只有一个线程可以向下游广播，确保下游只收到一次
            if (this.index != 0) {
                return;
            }
        }

        this.channelList.forEach(channel -> {
            long seq = channel.getNextSeq();
            DataEvent dataEvent = channel.getBySeq(seq);
            dataEvent.setDataType(DataType.BROADCAST);
            dataEvent.setData(broadcastData);
            channel.pushBySeq(seq);
        });
    }


    public V feed(K key) {

        this.tmpChannel = selectChannel(key);
        this.sequence = this.tmpChannel.getNextSeq();
        DataEvent<V> event = this.tmpChannel.getBySeq(this.sequence);
        if (event.getDataType() != DataType.DATA) {
            event.setDataType(DataType.DATA);
            event.setData((V) this.dataEventFactory.createData());
        }
        return event.getData();
    }


    private RingbufferChannel selectChannel(K key) {

        if (this.availbleNum.get() == 1) {
            return this.channelList.get(0);
        }
        int intVal = this.selector.getIntVal(key);
        intVal = Math.abs(intVal);
        if (intVal < 0) {
            intVal = 0;
        }
        if (intVal >= this.availbleNum.get()) {
            intVal = intVal % this.availbleNum.get();
        }
        this.tmpChannel = this.channelList.get(intVal);
        if (this.dynamicBalance && this.tmpChannel.remainCapacity() < BALANCE_LEVEL) {
            return findIdleOne();
        }
        return this.tmpChannel;
    }


    private RingbufferChannel findIdleOne() {

        long remain = -1;
        int index = 0;
        for (int i = 0; i < this.availbleNum.get(); i++) {
            long t = this.channelList.get(i).remainCapacity();
            if (t > BALANCE_LEVEL) {
                index = i;
                break;
            } else {
                if (remain < t) {
                    remain = t;
                    index = i;
                }
            }
        }
        return this.tmpChannel = this.channelList.get(index);
    }


    public void push() {

        this.tmpChannel.pushBySeq(this.sequence);
    }
}
