package com.github.fevernova.framework.autoscale;


import com.github.fevernova.framework.autoscale.status.*;
import com.github.fevernova.framework.component.ComponentType;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class Agent extends IStatus {


    @Getter
    protected List<IStatus> statesRules = Lists.newArrayList();

    @Getter
    protected IStatus currentStatus;

    protected ActionTags curActionTags;

    private AtomicInteger currentNum;


    public Agent(ComponentType componentType, int upperBound, int lowerBound, AtomicInteger currentNum) {

        super(componentType, upperBound, lowerBound, currentNum.get());
        provideStateRules();
        traverseRules();
        this.currentStatus = matchStatus(upperBound, lowerBound, currentNum.get());
        this.curActionTags = ActionTags.INVARIANT;
        this.currentNum = currentNum;
    }


    protected void provideStateRules() {

        this.statesRules.add(new FixedStatus(super.componentType, 1, 1, 1));
        this.statesRules.add(new MinStatus(super.componentType, 3, 1, 1));
        this.statesRules.add(new BriskStatus(super.componentType, 3, 1, 2));
        this.statesRules.add(new MaxStatus(super.componentType, 3, 1, 3));

    }


    private void traverseRules() {

        for (int ct = super.lowerBound; ct <= super.upperBound; ct++) {
            int r = 0;
            for (IStatus status : this.statesRules) {
                IStatus result = status.matchStatus(super.upperBound, super.lowerBound, ct);
                if (result != null) {
                    r++;
                }
            }
            Validate.isTrue(r == 1, "TraverseRule Failed : " + ct);
        }
    }


    @Override public IStatus matchStatus(int upperBound, int lowerBound, int cur) {

        for (IStatus status : this.statesRules) {
            IStatus result = status.matchStatus(upperBound, lowerBound, cur);
            if (result != null) {
                return result;
            }
        }
        Validate.isTrue(false, "Not Match State");
        return null;
    }


    @Override public void addTimeLine(Tendency tendency) {

        this.currentStatus.addTimeLine(tendency);
    }


    @Override
    public Pair<ComponentType, ActionTags> triggerAction(int lastNumber, int highScore, int lowScore) {

        Pair<ComponentType, ActionTags> action = this.currentStatus.triggerAction(lastNumber, highScore, lowScore);
        this.curActionTags = action.getRight();
        return action;
    }


    @Override protected ActionTags inc() {

        return null;
    }


    @Override protected ActionTags dec() {

        return null;
    }


    public void toNextState() {

        if (this.curActionTags.change) {
            IStatus status = this.matchStatus(super.upperBound, super.lowerBound, this.currentNum.get());
            this.currentStatus = status;
            super.cur = this.currentNum.get();
        }
    }
}
