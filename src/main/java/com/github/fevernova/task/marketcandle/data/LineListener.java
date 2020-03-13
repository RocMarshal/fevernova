package com.github.fevernova.task.marketcandle.data;


import com.github.fevernova.framework.window.WindowListener;
import lombok.Getter;


public class LineListener implements WindowListener<Point> {


    private Point cache;

    @Getter
    private Point maxPoint;


    public Point getAndClear() {

        Point p = this.cache;
        this.cache = null;
        return p;
    }


    @Override public void createNewWindow(Point window) {

        if (this.maxPoint == null || window.getId() > this.maxPoint.getId()) {
            this.maxPoint = window;
        }
    }


    @Override public void removeOldWindow(Point window) {

        window.setCompleted(true);
        this.cache = window;
    }
}
