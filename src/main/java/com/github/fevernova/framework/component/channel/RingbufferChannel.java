package com.github.fevernova.framework.component.channel;


import com.github.fevernova.framework.common.Constants;
import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.DataEvent;
import com.github.fevernova.framework.component.PExceptionHandler;
import com.github.fevernova.framework.component.Processor;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang3.Validate;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class RingbufferChannel {


    private RingBuffer<DataEvent> ringBuffer;

    private GlobalContext globalContext;

    private SequenceBarrier seqBarrier;

    private EventProcessor eventProcessor;

    private WaitStrategy strategy;

    private AtomicBoolean STARTED = new AtomicBoolean(false);


    public RingbufferChannel(int producerNum, TaskContext channelContext, GlobalContext globalContext, EventFactory<DataEvent> eventFactory) {

        this.globalContext = globalContext;
        int bufferSize = channelContext.getInteger(Constants.SIZE, 512);
        long timeout = channelContext.getLong("timeout", 1000L);
        this.strategy = new CustomWaitStrategy(timeout, TimeUnit.MILLISECONDS);

        this.ringBuffer = RingBuffer.create((producerNum == 1 ? ProducerType.SINGLE : ProducerType.MULTI), eventFactory, bufferSize, this.strategy);
        this.seqBarrier = this.ringBuffer.newBarrier();
    }


    public EventProcessor integrate(Processor processor) {

        Validate.isTrue(this.STARTED.compareAndSet(false, true));
        if (processor instanceof WaitNotify && this.strategy instanceof CustomWaitStrategy) {
            ((CustomWaitStrategy) this.strategy).setWaitNotify(processor);
        }
        Sequence consumerSequence = new Sequence(-1);
        this.eventProcessor =
                new WorkProcessor<>(this.ringBuffer, this.seqBarrier, processor, new PExceptionHandler(this.globalContext), consumerSequence);
        this.ringBuffer.addGatingSequences(this.eventProcessor.getSequence());
        return this.eventProcessor;
    }


    public long getNextSeq() {

        return this.ringBuffer.next();
    }


    public DataEvent getBySeq(long seq) {

        return this.ringBuffer.get(seq);
    }


    public void pushBySeq(long seq) {

        this.ringBuffer.publish(seq);
    }


    public boolean isEmpty() {

        return this.ringBuffer.remainingCapacity() == this.ringBuffer.getBufferSize();
    }


    public long remainCapacity() {

        return this.ringBuffer.remainingCapacity();
    }


    public long getBufferSize() {

        return this.ringBuffer.getBufferSize();
    }
}
