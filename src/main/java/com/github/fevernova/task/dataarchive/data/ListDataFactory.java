package com.github.fevernova.task.dataarchive.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class ListDataFactory implements DataFactory {


    @Override public Data createData() {

        return new ListData();
    }
}
