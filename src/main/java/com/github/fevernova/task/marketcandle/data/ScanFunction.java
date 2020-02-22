package com.github.fevernova.task.marketcandle.data;


import java.util.List;


public interface ScanFunction {


    void onChange(Integer symbolId, List<Point> points);

}
