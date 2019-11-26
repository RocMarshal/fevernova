package com.github.fevernova.framework.common.data;


import com.lmax.disruptor.EventFactory;


public class DataEventFactory<T extends Data> implements EventFactory<DataEvent>, DataFactory<T> {


    private DataFactory<T> dataFactory;


    public DataEventFactory(DataFactory dataFactory) {

        this.dataFactory = dataFactory;
    }


    @Override public DataEvent newInstance() {

        DataEvent dataEvent = new DataEvent();
        dataEvent.setData(createData());
        return dataEvent;
    }


    @Override public T createData() {

        return this.dataFactory.createData();
    }
}
