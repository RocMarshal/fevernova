package com.github.fevernova.framework.component;


import com.github.fevernova.framework.service.state.StateValue;
import com.lmax.disruptor.LifecycleAware;

import java.util.List;


public abstract class LifecycleAwareExtension implements LifecycleAware {


    public abstract void init();


    @Override public void onStart() {

    }


    public void onRecovery(List<StateValue> stateValueList) {

    }


    public abstract void onPause();

    public abstract void onResume();


    @Override public void onShutdown() {

    }
}
