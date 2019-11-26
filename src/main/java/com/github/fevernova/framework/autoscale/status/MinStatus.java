package com.github.fevernova.framework.autoscale.status;


import com.github.fevernova.framework.autoscale.ActionTags;
import com.github.fevernova.framework.component.ComponentType;


public class MinStatus extends IStatus {


    public MinStatus(ComponentType componentType, int upperBound, int lowerBound, int cur) {

        super(componentType, upperBound, lowerBound, cur);
    }


    @Override public IStatus matchStatus(int upperBound, int lowerBound, int cur) {

        if (upperBound > lowerBound && cur == lowerBound) {
            return new MinStatus(super.componentType, upperBound, lowerBound, cur);
        }
        return null;
    }


    @Override protected ActionTags inc() {

        return ActionTags.EXPAND;
    }


    @Override protected ActionTags dec() {

        return ActionTags.ABATEMENT;
    }

}
