package com.github.fevernova.framework.window;


public interface WindowListener<W extends ObjectWithId> {


    void createNewWindow(W window);

    void removeOldWindow(W window);

}
