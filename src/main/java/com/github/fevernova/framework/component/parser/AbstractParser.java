package com.github.fevernova.framework.component.parser;


import com.github.fevernova.framework.common.context.GlobalContext;
import com.github.fevernova.framework.common.context.TaskContext;
import com.github.fevernova.framework.common.data.BarrierData;
import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.broadcast.BroadcastData;
import com.github.fevernova.framework.component.DataProvider;
import com.github.fevernova.framework.component.Processor;
import com.github.fevernova.framework.component.channel.ChannelProxy;
import org.apache.commons.lang3.Validate;


public abstract class AbstractParser<K, V extends Data> extends Processor implements DataProvider<K, V> {


    protected ChannelProxy<K, V> channelProxy;


    public AbstractParser(GlobalContext globalContext, TaskContext taskContext, int index, int inputsNum, ChannelProxy channelProxy) {

        super(globalContext, taskContext, index, inputsNum);
        this.channelProxy = channelProxy;
    }


    @Override
    public V feedOne(K key) {

        return this.channelProxy.feed(key);
    }


    @Override
    public void push() {

        this.channelProxy.push();
    }


    @Override protected void snapshotWhenBarrier(BarrierData barrierData) {

    }


    @Override protected void distributeBarrier(BarrierData barrierData) {

        this.channelProxy.distributeBarrier(barrierData);
    }


    @Override public void onBroadcastData(BroadcastData broadcastData) {

        BroadcastData newBroadcastData = onBroadcast(broadcastData);
        Validate.notNull(newBroadcastData);
        if (newBroadcastData.global) {
            broadcast(newBroadcastData);
        }
    }


    @Override protected BroadcastData onBroadcast(BroadcastData broadcastData) {

        return broadcastData;
    }


    public void broadcast(BroadcastData broadcastData) {

        this.channelProxy.broadcast(broadcastData);
    }

}
