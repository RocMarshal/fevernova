package com.github.fevernova.task.exchange.data.result;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class OrderMatchFactory implements DataFactory {


    @Override public Data createData() {

        OrderMatch orderMatch = new OrderMatch();
        orderMatch.setPart0(new OrderMatchPart());
        orderMatch.setPart1(new OrderMatchPart());
        return orderMatch;
    }
}
