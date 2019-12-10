package com.github.fevernova.framework.autoscale.status;


import com.github.fevernova.framework.autoscale.ActionTags;
import com.github.fevernova.framework.autoscale.Tendency;
import com.github.fevernova.framework.common.structure.list.AnnularArray;
import com.github.fevernova.framework.component.ComponentType;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;


public abstract class IStatus {


    @Getter
    protected ComponentType componentType;

    protected int upperBound;

    protected int lowerBound;

    protected int cur;

    @Getter
    private AnnularArray<Integer> timeLine;


    public IStatus(ComponentType componentType, int upperBound, int lowerBound, int cur) {

        this.componentType = componentType;
        this.upperBound = upperBound;
        this.lowerBound = lowerBound;
        this.cur = cur;
        this.timeLine = new AnnularArray(10);
    }


    public abstract IStatus matchStatus(int upperBound, int lowerBound, int cur);


    public void addTimeLine(Tendency tendency) {

        this.timeLine.add(tendency.num);
    }


    public Pair<ComponentType, ActionTags> triggerAction(int lastNumber, int highScore, int lowScore) {

        int loadEvaluator = 0;

        for (int k = lastNumber - 1; k > 0; k--) {
            loadEvaluator = loadEvaluator + (this.timeLine.getByReverseIndex(k) != null ? this.timeLine.getByReverseIndex(k) : 0);
        }

        if (loadEvaluator >= highScore) {
            return Pair.of(this.componentType, inc());
        } else if (loadEvaluator <= lowScore) {
            return Pair.of(this.componentType, dec());
        } else {
            return Pair.of(this.componentType, stay());
        }
    }


    protected abstract ActionTags inc();

    protected abstract ActionTags dec();


    protected ActionTags stay() {

        return ActionTags.INVARIANT;
    }

}
