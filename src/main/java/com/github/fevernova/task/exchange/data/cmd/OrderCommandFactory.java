package com.github.fevernova.task.exchange.data.cmd;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class OrderCommandFactory implements DataFactory {


    @Override public Data createData() {

        return new OrderCommand();
    }
}
