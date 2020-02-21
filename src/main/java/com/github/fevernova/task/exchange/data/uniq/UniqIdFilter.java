package com.github.fevernova.task.exchange.data.uniq;


import com.github.fevernova.task.exchange.window.SlideWindow;


public class UniqIdFilter extends SlideWindow<UniqIdData> {


    public UniqIdFilter(long span, int windowNum) {

        super(span, windowNum);
    }


    public boolean unique(long timestamp, long eventId) {

        if (prepareCurrentWindow(timestamp)) {
            return super.currentWindow.unique(eventId);
        }
        return false;
    }


    @Override protected UniqIdData newWindow(int seq) {

        return new UniqIdData(seq);
    }
}
