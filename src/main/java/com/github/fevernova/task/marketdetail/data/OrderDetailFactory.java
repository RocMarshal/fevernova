package com.github.fevernova.task.marketdetail.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class OrderDetailFactory implements DataFactory {


    @Override public Data createData() {

        return new OrderDetail();
    }
}
