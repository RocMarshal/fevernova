package com.github.fevernova.framework.component.channel;


import com.lmax.disruptor.*;
import lombok.Setter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class CustomWaitStrategy implements WaitStrategy {


    private final Lock lock = new ReentrantLock();

    private final Condition processorNotifyCondition = lock.newCondition();

    private final AtomicBoolean signalNeeded = new AtomicBoolean(false);

    private final long timeoutInNanos;

    @Setter
    private WaitNotify waitNotify = new WaitNotify.IgnoreWaitNotify();


    public CustomWaitStrategy(final long timeout, final TimeUnit units) {

        timeoutInNanos = units.toNanos(timeout);
    }


    @Override
    public long waitFor(final long sequence, final Sequence cursorSequence, final Sequence dependentSequence, final SequenceBarrier barrier)
            throws AlertException, InterruptedException, TimeoutException {

        long nanos = timeoutInNanos;

        long availableSequence;
        if (cursorSequence.get() < sequence) {
            lock.lock();
            long st = nanos;
            try {
                while (cursorSequence.get() < sequence) {
                    signalNeeded.getAndSet(true);

                    barrier.checkAlert();
                    nanos = processorNotifyCondition.awaitNanos(nanos);
                    if (nanos <= 0) {
                        throw TimeoutException.INSTANCE;
                    }
                }
            } finally {
                lock.unlock();
                long delta = st - nanos;
                waitNotify.waitTime(delta);

            }
        }

        while ((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
        }

        return availableSequence;
    }


    @Override
    public void signalAllWhenBlocking() {

        if (signalNeeded.getAndSet(false)) {
            lock.lock();
            try {
                processorNotifyCondition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }


    @Override
    public String toString() {

        return "LiteTimeoutBlockingWaitStrategy{processorNotifyCondition=" + processorNotifyCondition + '}';
    }
}
