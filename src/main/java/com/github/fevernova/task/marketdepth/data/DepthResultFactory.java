package com.github.fevernova.task.marketdepth.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class DepthResultFactory implements DataFactory {


    @Override public Data createData() {

        return new DepthResult();
    }
}
