package com.github.fevernova.framework.component;


public enum ComponentStatus {

    INIT(false), RUNNING(true), PAUSE(true), CLOSING(false);

    public final boolean inLoop;


    ComponentStatus(boolean inLoop) {

        this.inLoop = inLoop;
    }

}
