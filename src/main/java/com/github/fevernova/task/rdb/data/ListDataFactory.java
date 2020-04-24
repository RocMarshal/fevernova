package com.github.fevernova.task.rdb.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class ListDataFactory implements DataFactory {


    @Override public Data createData() {

        return new ListData();
    }
}
