package com.github.fevernova.framework.component.channel;


public interface WaitNotify {


    void waitTime(long nanos);

    public class IgnoreWaitNotify implements WaitNotify {


        public void waitTime(long nanos) {

        }
    }

}
