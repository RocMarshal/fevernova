package com.github.fevernova.task.marketcandle.data;


import com.github.fevernova.framework.common.data.Data;
import com.github.fevernova.framework.common.data.DataFactory;


public class CandleDiffFactory implements DataFactory {


    @Override public Data createData() {

        return new CandleDiff();
    }
}
