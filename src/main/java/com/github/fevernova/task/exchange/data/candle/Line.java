package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.data.candle.Point;
import com.github.fevernova.task.exchange.window.SlideWindow;


public class Line extends SlideWindow<Point> {


    public Line(long span, int windowNum) {

        super(span, windowNum);
    }


    public void acc(long timestamp, long price, long size) {

        prepareCurrentWindow(timestamp);
        super.currentWindow.acc(price, size);
    }


    @Override protected Point newWindow(int seq) {

        return new Point(seq);
    }
}
