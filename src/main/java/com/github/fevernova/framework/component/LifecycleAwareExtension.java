package com.github.fevernova.framework.component;


import com.lmax.disruptor.LifecycleAware;


public abstract class LifecycleAwareExtension implements LifecycleAware {


    public abstract void init();


    @Override public void onStart() {

    }


    public void onRecovery() {

    }
    

    public abstract void onPause();

    public abstract void onResume();


    @Override public void onShutdown() {

    }
}
