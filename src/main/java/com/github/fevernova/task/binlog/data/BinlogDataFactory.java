package com.github.fevernova.task.binlog.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class BinlogDataFactory implements DataFactory {


    @Override public Data createData() {

        return new BinlogData();
    }
}
