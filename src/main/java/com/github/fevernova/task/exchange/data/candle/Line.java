package com.github.fevernova.task.exchange.data.candle;


import com.github.fevernova.task.exchange.window.SlideWindow;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.function.Consumer;


public class Line extends SlideWindow<Point> {


    public Line(long span, int windowNum) {

        super(span, windowNum, new CacheListener());
    }


    public void acc(long timestamp, long price, long size, long sequence) {

        if (prepareCurrentWindow(timestamp)) {
            super.currentWindow.acc(price, size, sequence);
        }
    }


    @Override protected Point newWindow(int seq) {

        return new Point(seq);
    }


    public Point pollRemoved() {

        return ((CacheListener) super.windowListener).getAndClear();
    }


    public List<Point> scan4Update() {

        final List<Point> result = Lists.newArrayListWithCapacity(2);
        super.windows.forEach((Consumer<Point>) point -> {
            if (point.isUpdate()) {
                result.add(point.copyByScan());
            }
        });
        return result;
    }
}
