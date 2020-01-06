package com.github.fevernova.framework.component.channel.selector;


public class LongSelector implements ISelector<Long> {


    @Override public int getIntVal(Long val) {

        return val.intValue();
    }
}
