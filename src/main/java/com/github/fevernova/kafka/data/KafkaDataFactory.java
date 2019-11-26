package com.github.fevernova.kafka.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class KafkaDataFactory implements DataFactory {


    @Override public Data createData() {

        return new KafkaData();
    }
}
