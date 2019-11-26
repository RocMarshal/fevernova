package com.github.fevernova.framework.autoscale;


import com.github.fevernova.framework.component.ComponentChangeMode;


public enum ActionTags {

    ABATEMENT(false, null), //外部收缩
    SHRINK(true, ComponentChangeMode.DECREMENT),  //减少组件的并行度
    INVARIANT(false, null), // 不变
    EXPAND(true, ComponentChangeMode.INCREMENT),  //增加组件并行度
    AUGMENT(false, null); //外部扩容

    public final boolean change;

    public final ComponentChangeMode componentChangeMode;


    ActionTags(boolean change, ComponentChangeMode componentChangeMode) {

        this.change = change;
        this.componentChangeMode = componentChangeMode;
    }

}
