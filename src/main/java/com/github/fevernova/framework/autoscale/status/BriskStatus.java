package com.github.fevernova.framework.autoscale.status;


import com.github.fevernova.framework.autoscale.ActionTags;
import com.github.fevernova.framework.component.ComponentType;


public class BriskStatus extends IStatus {


    public BriskStatus(ComponentType componentType, int upperBound, int lowerBound, int cur) {

        super(componentType, upperBound, lowerBound, cur);
    }


    @Override public IStatus matchStatus(int upperBound, int lowerBound, int cur) {

        if (upperBound > lowerBound && cur > lowerBound && cur < upperBound) {
            return new BriskStatus(super.componentType, upperBound, lowerBound, cur);
        }
        return null;
    }


    @Override protected ActionTags inc() {

        return ActionTags.EXPAND;
    }


    @Override protected ActionTags dec() {

        return ActionTags.SHRINK;
    }

}
