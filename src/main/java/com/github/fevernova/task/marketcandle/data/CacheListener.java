package com.github.fevernova.task.marketcandle.data;


import com.github.fevernova.framework.window.WindowListener;


public class CacheListener implements WindowListener<Point> {


    private Point cache;


    public Point getAndClear() {

        Point p = this.cache;
        this.cache = null;
        return p;
    }


    @Override public void createNewWindow(Point window) {

    }


    @Override public void removeOldWindow(Point window) {

        window.setCompleted(true);
        this.cache = window;
    }
}
