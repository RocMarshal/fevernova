package com.github.fevernova.io.mysql.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class ListDataFactory implements DataFactory {


    @Override public Data createData() {

        return new ListData();
    }
}
