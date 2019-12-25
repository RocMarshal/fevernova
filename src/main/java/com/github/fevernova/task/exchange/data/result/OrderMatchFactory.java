package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class OrderMatchFactory implements DataFactory {


    @Override public Data createData() {

        return new OrderMatch();
    }
}
