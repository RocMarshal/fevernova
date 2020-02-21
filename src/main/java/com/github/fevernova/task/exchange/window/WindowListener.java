package com.github.fevernova.task.exchange.window;


public interface WindowListener<W extends ObjectWithId> {


    void createNewWindow(W window);

    void removeOldWindow(W window);

}
