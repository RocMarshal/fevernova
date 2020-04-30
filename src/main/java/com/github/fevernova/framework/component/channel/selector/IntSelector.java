package com.github.fevernova.framework.component.channel.selector;


public class IntSelector implements ISelector<Integer> {


    @Override
    public int getIntVal(Integer val) {

        return val == null ? 0 : val;
    }
}
