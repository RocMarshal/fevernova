package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.window.WindowListener;


public class CacheListener implements WindowListener<Point> {


    private Point cache;


    public Point getAndClear() {

        Point p = this.cache;
        this.cache = null;
        return p;
    }


    @Override public void removeOldWindow(Point window) {

        this.cache = window;
    }


    @Override public void createNewWindow(Point window) {

    }
}
