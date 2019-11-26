package com.github.fevernova.framework.autoscale.status;


import com.github.fevernova.framework.autoscale.ActionTags;
import com.github.fevernova.framework.component.ComponentType;


public class MaxStatus extends IStatus {


    public MaxStatus(ComponentType componentType, int upperBound, int lowerBound, int cur) {

        super(componentType, upperBound, lowerBound, cur);
    }


    @Override public IStatus matchStatus(int upperBound, int lowerBound, int cur) {

        if (upperBound > lowerBound && cur == upperBound) {
            return new MaxStatus(super.componentType, upperBound, lowerBound, cur);
        }
        return null;
    }


    @Override protected ActionTags inc() {

        return ActionTags.AUGMENT;
    }


    @Override protected ActionTags dec() {

        return ActionTags.SHRINK;
    }

}
