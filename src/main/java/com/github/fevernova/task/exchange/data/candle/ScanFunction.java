package com.github.fevernova.task.exchange.data.candle;


import java.util.List;


public interface ScanFunction {


    void onUpdate(Integer symbolId, List<Point> points);

    void onRemove(Integer symbolId, List<Point> points);
}
